"""Pipeline steps. Each step is independently runnable (Airflow runs one per task)
and safe to re-run for the same batch date."""

from __future__ import annotations

import datetime as dt
import json
import logging

import psycopg

from lakehouse import bronze, generate
from lakehouse.config import Settings
from lakehouse.db import ensure_database, run_sql_file

logger = logging.getLogger(__name__)

STEPS = ("generate", "bronze", "silver", "quality", "load", "dbt", "reconcile")


def init(settings: Settings) -> None:
    """Create databases and base schemas (idempotent)."""
    ensure_database(settings.source_db_url)
    ensure_database(settings.warehouse_db_url)
    run_sql_file(settings.source_db_url, "source_schema.sql")
    run_sql_file(settings.warehouse_db_url, "warehouse_init.sql")


def step_generate(settings: Settings, batch_date: dt.date) -> dict:
    out = generate.simulate(batch_date)
    folder = generate.write_landing_files(out, settings.landing_root)
    inserted = generate.write_source_db(out, settings.source_db_url)
    return {"landing": str(folder), "source_rows_written": inserted,
            "orders": len(out.orders), "order_items": len(out.order_items)}


def step_bronze(settings: Settings, batch_date: dt.date) -> dict:
    window = bronze.register_batch(settings, batch_date)
    counts = bronze.ingest_csv(settings, batch_date)
    counts.update(bronze.ingest_postgres(settings, batch_date, window))
    return counts


def step_silver(settings: Settings, batch_date: dt.date) -> dict:
    from lakehouse.silver import run_silver
    from lakehouse.spark import get_spark

    spark = get_spark(settings)
    try:
        metrics = run_silver(spark, settings, batch_date)
    finally:
        spark.stop()
    return {k: v.silver_rows_written for k, v in metrics.items()}


def step_quality(settings: Settings, batch_date: dt.date) -> dict:
    from lakehouse.quality import run_quality

    report = run_quality(settings, batch_date)
    return {"checks": len(report.checks), "warnings": sum(not c.success for c in report.checks)}


def step_load(settings: Settings, batch_date: dt.date) -> dict:
    from lakehouse.load import run_load

    return run_load(settings, batch_date)


def step_dbt(settings: Settings, batch_date: dt.date) -> dict:
    from lakehouse.dbt_runner import run_dbt

    run_dbt(settings, "seed")
    run_dbt(settings, "snapshot")
    run_dbt(settings, "run")
    run_dbt(settings, "test")
    return {"dbt": "seed, snapshot, run, test passed"}


def _window(settings: Settings, batch_date: dt.date) -> bronze.Window:
    with psycopg.connect(settings.warehouse_db_url) as conn:
        row = conn.execute("SELECT window_start, window_end FROM meta.ingestion_batches WHERE batch_id = %s",
                           (batch_date,)).fetchone()
    if not row:
        raise RuntimeError(f"Batch {batch_date} was never registered; run the bronze step first.")
    return bronze.Window(*row)


def step_reconcile(settings: Settings, batch_date: dt.date) -> dict:
    from lakehouse.reconcile import run_reconciliation

    report = run_reconciliation(settings, batch_date, _window(settings, batch_date))
    _mark_batch(settings, batch_date, "succeeded")
    return {"checks_passed": len(report.checks)}


def _mark_batch(settings: Settings, batch_date: dt.date, status: str, metrics: dict | None = None) -> None:
    with psycopg.connect(settings.warehouse_db_url) as conn:
        conn.execute(
            "UPDATE meta.ingestion_batches SET status = %s, finished_at = now(), "
            "metrics = coalesce(%s::jsonb, metrics) WHERE batch_id = %s",
            (status, json.dumps(metrics) if metrics else None, batch_date),
        )
        conn.commit()


STEP_FUNCTIONS = {
    "generate": step_generate,
    "bronze": step_bronze,
    "silver": step_silver,
    "quality": step_quality,
    "load": step_load,
    "dbt": step_dbt,
    "reconcile": step_reconcile,
}


def run_batch(settings: Settings, batch_date: dt.date, *, skip_generate: bool = False) -> dict:
    init(settings)
    results = {}
    for name in STEPS:
        if name == "generate" and skip_generate:
            continue
        try:
            results[name] = STEP_FUNCTIONS[name](settings, batch_date)
        except Exception:
            if name not in ("generate",):
                try:
                    _mark_batch(settings, batch_date, "failed")
                except Exception:  # pragma: no cover - best effort
                    logger.exception("could not mark batch failed")
            raise
        logger.info("step %-9s done: %s", name, results[name])
    _mark_batch(settings, batch_date, "succeeded", results)
    return results
