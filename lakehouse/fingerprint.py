"""Content fingerprints of warehouse tables, used to prove idempotent re-runs."""

from __future__ import annotations

import psycopg
from psycopg import sql

TABLES = [
    ("lake_silver", "customers"), ("lake_silver", "orders"), ("lake_silver", "order_items"),
    ("lake_silver", "inventory"), ("analytics_snapshots", "customers_snapshot"),
    ("analytics_marts", "dim_customers"), ("analytics_marts", "fct_orders"),
    ("analytics_marts", "fct_order_items"), ("analytics_marts", "fct_inventory_daily"),
]
# Columns whose values legitimately change per run (load timestamps) are excluded.
VOLATILE = {"dbt_updated_at"}


def fingerprint(url: str) -> dict[str, tuple[int, str]]:
    """Return {schema.table: (row_count, md5 of all rows in a canonical order)}."""
    out = {}
    with psycopg.connect(url) as conn:
        for schema, table in TABLES:
            cols = [r[0] for r in conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_schema=%s AND table_name=%s "
                "ORDER BY ordinal_position", (schema, table)).fetchall() if r[0] not in VOLATILE]
            col_list = sql.SQL(", ").join(map(sql.Identifier, cols))
            query = sql.SQL(
                "SELECT count(*), coalesce(md5(string_agg(r::text, '|' ORDER BY r::text)), '') "
                "FROM (SELECT {cols} FROM {tbl}) r"
            ).format(cols=col_list, tbl=sql.Identifier(schema, table))
            count, digest = conn.execute(query).fetchone()
            out[f"{schema}.{table}"] = (count, digest)
    return out
