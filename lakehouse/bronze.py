"""Bronze layer: land raw source data as Parquet, unchanged except for lineage columns.

* CSV exports are read as strings (schema-on-read; typing happens in Silver).
* OLTP tables are extracted incrementally by ``updated_at`` within a batch window.

Every write replaces the ``batch_id=<date>`` partition, so re-running a batch is idempotent.
"""

from __future__ import annotations

import datetime as dt
import logging
import shutil
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import psycopg

from lakehouse.config import Settings

logger = logging.getLogger(__name__)

CSV_ENTITIES = ("customers", "products", "inventory")
DB_ENTITIES = {"orders": "sales.orders", "order_items": "sales.order_items"}
EPOCH = dt.datetime(1970, 1, 1)


class OutOfOrderBatchError(RuntimeError):
    """Raised when re-running a batch older than the latest succeeded batch.

    Snapshot entities (customers, products) are replaced wholesale, so replaying an older
    batch would roll the warehouse's current state back in time.
    """


@dataclass(frozen=True)
class Window:
    start: dt.datetime  # exclusive
    end: dt.datetime    # inclusive upper bound is `< end`


def compute_window(batch_date: dt.date, previous: list[tuple[dt.date, dt.datetime, dt.datetime, str]]) -> Window:
    """Pure function deciding the extraction window for ``batch_date``.

    ``previous`` holds (batch_id, window_start, window_end, status) rows from meta.ingestion_batches.
    * A batch that was run before keeps its original window (idempotent re-runs).
    * Otherwise the window starts where the latest *succeeded* earlier batch ended, so
      data from failed batches is caught up automatically.
    """
    for batch_id, start, end, _status in previous:
        if batch_id == batch_date:
            return Window(start, end)
    ends = [end for batch_id, _s, end, status in previous if status == "succeeded" and batch_id < batch_date]
    start = max(ends) if ends else EPOCH
    end = dt.datetime.combine(batch_date, dt.time())
    if start > end:
        raise ValueError(f"Window start {start} is after end {end}; batches must run in date order.")
    return Window(start, end)


def register_batch(settings: Settings, batch_date: dt.date) -> Window:
    with psycopg.connect(settings.warehouse_db_url) as conn:
        rows = conn.execute(
            "SELECT batch_id, window_start, window_end, status FROM meta.ingestion_batches"
        ).fetchall()
        newer = sorted(b for b, _s, _e, status in rows if status == "succeeded" and b > batch_date)
        if newer:
            raise OutOfOrderBatchError(
                f"Batch {batch_date} is older than succeeded batch {newer[-1]}; re-running it would roll back "
                "snapshot tables. Re-run the latest batch instead, or rebuild from scratch for a backfill."
            )
        window = compute_window(batch_date, rows)
        conn.execute(
            """
            INSERT INTO meta.ingestion_batches (batch_id, window_start, window_end, status)
            VALUES (%s, %s, %s, 'running')
            ON CONFLICT (batch_id) DO UPDATE SET status = 'running', started_at = now(), finished_at = NULL
            """,
            (batch_date, window.start, window.end),
        )
        conn.commit()
    return window


def partition_path(root: Path, entity: str, batch_date: dt.date) -> Path:
    return root / entity / f"batch_id={batch_date.isoformat()}"


def _write_partition(frame: pd.DataFrame, path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)
    frame.to_parquet(path / "part-0.parquet", index=False)


def _lineage(frame: pd.DataFrame, batch_date: dt.date, source: str) -> pd.DataFrame:
    frame = frame.copy()
    frame["_source"] = source
    frame["_batch_id"] = batch_date.isoformat()
    return frame


def ingest_csv(settings: Settings, batch_date: dt.date) -> dict[str, int]:
    folder = settings.landing_root / batch_date.isoformat()
    counts = {}
    for entity in CSV_ENTITIES:
        path = folder / f"{entity}.csv"
        if not path.exists():
            raise FileNotFoundError(f"Missing landing file {path}")
        frame = pd.read_csv(path, dtype=str, keep_default_na=False)
        _write_partition(_lineage(frame, batch_date, f"csv:{path.name}"),
                         partition_path(settings.bronze_root, entity, batch_date))
        counts[entity] = len(frame)
        logger.info("bronze %-12s %6d rows from %s", entity, len(frame), path)
    return counts


def ingest_postgres(settings: Settings, batch_date: dt.date, window: Window) -> dict[str, int]:
    counts = {}
    with psycopg.connect(settings.source_db_url) as conn:
        for entity, table in DB_ENTITIES.items():
            cur = conn.execute(
                f"SELECT * FROM {table} WHERE updated_at > %s AND updated_at < %s ORDER BY 1",  # noqa: S608
                (window.start, window.end),
            )
            columns = [d.name for d in cur.description]
            frame = pd.DataFrame(cur.fetchall(), columns=columns)
            # Bronze keeps raw values; store as strings so Silver owns all typing decisions.
            frame = frame.astype("string").fillna("")
            _write_partition(_lineage(frame, batch_date, f"postgres:{table}"),
                             partition_path(settings.bronze_root, entity, batch_date))
            counts[entity] = len(frame)
            logger.info("bronze %-12s %6d rows from %s (%s, %s)", entity, len(frame), table,
                        window.start, window.end)
    return counts
