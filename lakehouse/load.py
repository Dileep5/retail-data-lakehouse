"""Load Silver Parquet into the PostgreSQL warehouse (schema ``lake_silver``).

* Snapshot entities (customers, products) are replaced inside one transaction.
* Orders, order items and inventory are upserted from the rows Silver touched in
  this batch (``_batch_id``), via COPY into a temp table + INSERT ... ON CONFLICT.

Both patterns are idempotent: loading the same batch twice yields the same tables.
"""

from __future__ import annotations

import datetime as dt
import logging

import pandas as pd
import psycopg
from psycopg import sql

from lakehouse.config import Settings

logger = logging.getLogger(__name__)

TABLES = {
    # entity: (columns, primary key, mode)
    "customers": (["customer_id", "first_name", "last_name", "email", "city", "province", "loyalty_tier",
                   "signup_date", "updated_at", "_batch_id"], ["customer_id"], "replace"),
    "products": (["product_id", "sku", "product_name", "category", "brand", "unit_price", "unit_cost",
                  "is_active", "updated_at", "_batch_id"], ["product_id"], "replace"),
    "orders": (["order_id", "customer_id", "store_id", "order_date", "channel", "status", "created_at",
                "updated_at", "_batch_id"], ["order_id"], "upsert"),
    "order_items": (["order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_pct",
                     "created_at", "updated_at", "_batch_id"], ["order_item_id"], "upsert"),
    "inventory": (["snapshot_date", "store_id", "product_id", "quantity_on_hand", "reorder_point", "_batch_id"],
                  ["snapshot_date", "store_id", "product_id"], "upsert"),
}


def _read(settings: Settings, entity: str, batch_date: dt.date, columns: list[str]) -> pd.DataFrame:
    frame = pd.read_parquet(settings.silver_root / entity, filters=[("_batch_id", "=", batch_date.isoformat())])
    if "snapshot_date" in frame.columns:  # partition column comes back as a category
        frame["snapshot_date"] = pd.to_datetime(frame["snapshot_date"].astype(str)).dt.date
    return frame[columns]


def _copy_rows(cur: psycopg.Cursor, table: sql.Composable, columns: list[str], frame: pd.DataFrame) -> None:
    col_sql = sql.SQL(", ").join(map(sql.Identifier, columns))
    with cur.copy(sql.SQL("COPY {} ({}) FROM STDIN").format(table, col_sql)) as copy:
        for row in frame.itertuples(index=False, name=None):
            copy.write_row([None if pd.isna(v) else v for v in row])


def load_entity(conn: psycopg.Connection, entity: str, frame: pd.DataFrame) -> int:
    columns, keys, mode = TABLES[entity]
    target = sql.Identifier("lake_silver", entity)
    with conn.cursor() as cur:
        if mode == "replace":
            cur.execute(sql.SQL("DELETE FROM {}").format(target))
            _copy_rows(cur, target, columns, frame)
        else:
            temp = sql.Identifier(f"tmp_load_{entity}")  # one per entity: all share a transaction
            cur.execute(sql.SQL("CREATE TEMP TABLE {} (LIKE {}) ON COMMIT DROP").format(temp, target))
            _copy_rows(cur, temp, columns, frame)
            updates = sql.SQL(", ").join(
                sql.SQL("{c} = EXCLUDED.{c}").format(c=sql.Identifier(c)) for c in columns if c not in keys
            )
            cur.execute(sql.SQL(
                "INSERT INTO {t} ({cols}) SELECT {cols} FROM {temp} "
                "ON CONFLICT ({keys}) DO UPDATE SET {updates}"
            ).format(
                t=target,
                temp=temp,
                cols=sql.SQL(", ").join(map(sql.Identifier, columns)),
                keys=sql.SQL(", ").join(map(sql.Identifier, keys)),
                updates=updates,
            ))
    return len(frame)


def run_load(settings: Settings, batch_date: dt.date) -> dict[str, int]:
    counts = {}
    # One transaction for the whole batch: the warehouse never sees a half-loaded batch.
    with psycopg.connect(settings.warehouse_db_url) as conn:
        for entity, (columns, _keys, _mode) in TABLES.items():
            counts[entity] = load_entity(conn, entity, _read(settings, entity, batch_date, columns))
            logger.info("load %-12s %6d rows", entity, counts[entity])
        conn.commit()
    return counts
