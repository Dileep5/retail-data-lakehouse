"""Cross-layer reconciliation: source -> Bronze -> Silver -> warehouse -> marts.

Checks (all must pass):
1. Extraction completeness: Bronze rows for the batch == source rows in the window.
2. Silver accounting: Bronze rows == quarantined + duplicates removed + Silver rows.
3. Load completeness: full Silver row counts == warehouse ``lake_silver`` row counts.
4. Revenue: completed-order revenue computed from Silver Parquet == sum of
   ``recognized_revenue`` in ``fct_orders``.
5. Dimension coverage: current versions in ``dim_customers`` == Silver customers.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
from dataclasses import asdict, dataclass, field
from decimal import Decimal

import pandas as pd
import psycopg

from lakehouse.bronze import DB_ENTITIES, Window, partition_path
from lakehouse.config import Settings
from lakehouse.silver import load_silver_metrics

logger = logging.getLogger(__name__)


class ReconciliationError(RuntimeError):
    pass


@dataclass
class Check:
    name: str
    expected: float
    actual: float
    passed: bool


@dataclass
class ReconciliationReport:
    batch_id: str
    checks: list[Check] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks)

    def add(self, name: str, expected, actual, tolerance: float = 0.0) -> None:
        expected, actual = float(expected), float(actual)
        self.checks.append(Check(name, expected, actual, abs(expected - actual) <= tolerance))


def silver_revenue(settings: Settings) -> Decimal:
    orders = pd.read_parquet(settings.silver_root / "orders", columns=["order_id", "status"])
    items = pd.read_parquet(settings.silver_root / "order_items",
                            columns=["order_id", "quantity", "unit_price", "discount_pct"])
    merged = items.merge(orders[orders["status"] == "completed"], on="order_id")
    # Same rounding as the dbt line_revenue macro: per line, half-up to cents.
    revenue = sum(
        (Decimal(q) * p * (1 - d / Decimal(100))).quantize(Decimal("0.01"), rounding="ROUND_HALF_UP")
        for q, p, d in merged[["quantity", "unit_price", "discount_pct"]].itertuples(index=False)
    )
    return Decimal(revenue)


def run_reconciliation(settings: Settings, batch_date: dt.date, window: Window) -> ReconciliationReport:
    report = ReconciliationReport(batch_id=batch_date.isoformat())

    with psycopg.connect(settings.source_db_url) as src:
        for entity, table in DB_ENTITIES.items():
            expected = src.execute(
                f"SELECT count(*) FROM {table} WHERE updated_at > %s AND updated_at < %s",  # noqa: S608
                (window.start, window.end),
            ).fetchone()[0]
            bronze = len(pd.read_parquet(partition_path(settings.bronze_root, entity, batch_date)))
            report.add(f"extract_complete:{entity}", expected, bronze)

    for entity, m in load_silver_metrics(settings, batch_date).items():
        report.add(f"silver_accounting:{entity}", m.bronze_rows,
                   m.quarantined + m.duplicates_removed + m.silver_rows_written)

    with psycopg.connect(settings.warehouse_db_url) as dw:
        for entity in ("customers", "products", "orders", "order_items", "inventory"):
            silver_rows = len(pd.read_parquet(settings.silver_root / entity, columns=["_batch_id"]))
            dw_rows = dw.execute(f"SELECT count(*) FROM lake_silver.{entity}").fetchone()[0]  # noqa: S608
            report.add(f"load_complete:{entity}", silver_rows, dw_rows)

        fact_revenue = dw.execute(
            "SELECT coalesce(sum(recognized_revenue), 0) FROM analytics_marts.fct_orders"
        ).fetchone()[0]
        report.add("revenue:silver_vs_fct_orders", silver_revenue(settings), fact_revenue, tolerance=0.01)

        current = dw.execute(  # customer_id -1 is the unknown member, not a real customer
            "SELECT count(*) FROM analytics_marts.dim_customers WHERE is_current AND customer_id <> -1"
        ).fetchone()[0]
        silver_customers = len(pd.read_parquet(settings.silver_root / "customers", columns=["customer_id"]))
        report.add("dim_customers_current_vs_silver", silver_customers, current)

    out = settings.metrics_root / f"batch_id={batch_date.isoformat()}" / "reconciliation.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({**asdict(report), "passed": report.passed}, indent=2, default=str))

    failed = [c for c in report.checks if not c.passed]
    if failed:
        raise ReconciliationError("; ".join(f"{c.name}: expected {c.expected}, got {c.actual}" for c in failed))
    logger.info("reconciliation passed (%d checks)", len(report.checks))
    return report
