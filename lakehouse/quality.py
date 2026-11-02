"""Data quality gate on Silver data using Great Expectations (1.x API).

Each entity has a suite of expectations tagged ``critical`` or ``warning``. A failed
critical expectation, or a quarantine rate above ``max_quarantine_ratio``, raises
``DataQualityError`` and stops the batch before anything reaches the warehouse.
Results are written to ``lake/_metrics/batch_id=<date>/quality.json``.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
from dataclasses import asdict, dataclass, field

import pandas as pd

from lakehouse.config import Settings
from lakehouse.silver import CHANNELS, PROVINCES, STATUSES, TIERS, load_silver_metrics

os.environ.setdefault("GX_ANALYTICS_ENABLED", "False")
os.environ.setdefault("TQDM_DISABLE", "1")
logging.getLogger("great_expectations").setLevel(logging.ERROR)  # before import: GX logs at import time

import great_expectations as gx  # noqa: E402
from great_expectations.data_context.types.base import ProgressBarsConfig  # noqa: E402

logger = logging.getLogger(__name__)
for _name in ("great_expectations", "great_expectations.data_context.types.base"):  # GX resets levels on import
    logging.getLogger(_name).setLevel(logging.ERROR)

E = gx.expectations


class DataQualityError(RuntimeError):
    pass


def suites(reference: dict[str, set]) -> dict[str, list[tuple[str, object]]]:
    """(severity, expectation) per entity. ``reference`` carries key sets for FK checks."""
    return {
        "customers": [
            ("critical", E.ExpectTableRowCountToBeBetween(min_value=1)),
            ("critical", E.ExpectColumnValuesToBeUnique(column="customer_id")),
            ("critical", E.ExpectColumnValuesToNotBeNull(column="customer_id")),
            ("critical", E.ExpectColumnValuesToMatchRegex(column="email", regex=r"^[^@\s]+@[^@\s]+\.[a-z]{2,}$")),
            ("critical", E.ExpectColumnValuesToBeInSet(column="loyalty_tier", value_set=TIERS)),
            ("critical", E.ExpectColumnValuesToBeInSet(column="province", value_set=PROVINCES)),
            ("warning", E.ExpectColumnValuesToBeUnique(column="email")),
        ],
        "products": [
            ("critical", E.ExpectTableRowCountToBeBetween(min_value=1)),
            ("critical", E.ExpectColumnValuesToBeUnique(column="product_id")),
            ("critical", E.ExpectColumnValuesToBeUnique(column="sku")),
            ("critical", E.ExpectColumnValuesToBeBetween(column="unit_price", min_value=0.01)),
            ("warning", E.ExpectColumnPairValuesAToBeGreaterThanB(column_A="unit_price", column_B="unit_cost")),
        ],
        "orders": [
            ("critical", E.ExpectColumnValuesToBeUnique(column="order_id")),
            ("critical", E.ExpectColumnValuesToBeInSet(column="status", value_set=STATUSES)),
            ("critical", E.ExpectColumnValuesToBeInSet(column="channel", value_set=CHANNELS)),
            ("critical", E.ExpectColumnValuesToBeBetween(column="store_id", min_value=1, max_value=8)),
            # Late-arriving customers are tolerated but surfaced.
            ("warning", E.ExpectColumnValuesToBeInSet(column="customer_id",
                                                      value_set=sorted(reference.get("customers", [])))),
        ],
        "order_items": [
            ("critical", E.ExpectColumnValuesToBeUnique(column="order_item_id")),
            ("critical", E.ExpectColumnValuesToBeBetween(column="quantity", min_value=1)),
            ("critical", E.ExpectColumnValuesToBeBetween(column="discount_pct", min_value=0, max_value=100)),
            ("warning", E.ExpectColumnValuesToBeInSet(column="product_id",
                                                      value_set=sorted(reference.get("products", [])))),
        ],
        "inventory": [
            ("critical", E.ExpectCompoundColumnsToBeUnique(column_list=["snapshot_date", "store_id", "product_id"])),
            ("critical", E.ExpectColumnValuesToBeBetween(column="quantity_on_hand", min_value=0)),
        ],
    }


@dataclass
class CheckResult:
    entity: str
    expectation: str
    column: str | None
    severity: str
    success: bool
    unexpected_count: int | None = None


@dataclass
class QualityReport:
    batch_id: str
    checks: list[CheckResult] = field(default_factory=list)
    quarantine_rates: dict[str, float] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return not self.errors


def validate_frame(entity: str, frame: pd.DataFrame, expectations: list[tuple[str, object]]) -> list[CheckResult]:
    context = gx.get_context(mode="ephemeral")
    context.variables.progress_bars = ProgressBarsConfig(globally=False)
    batch_def = (context.data_sources.add_pandas(f"silver_{entity}")
                 .add_dataframe_asset(name=entity).add_batch_definition_whole_dataframe("batch"))
    suite = context.suites.add(gx.ExpectationSuite(name=f"{entity}_suite"))
    severity_by_id = {}
    for severity, expectation in expectations:
        added = suite.add_expectation(expectation)
        severity_by_id[added.id] = severity
    result = batch_def.get_batch(batch_parameters={"dataframe": frame}).validate(suite)
    checks = []
    for r in result.results:
        cfg = r.expectation_config
        checks.append(CheckResult(
            entity=entity,
            expectation=cfg.type,
            column=cfg.kwargs.get("column") or ",".join(cfg.kwargs.get("column_list", []) or []) or None,
            severity=severity_by_id.get(cfg.id, "critical"),
            success=bool(r.success),
            unexpected_count=r.result.get("unexpected_count"),
        ))
    return checks


def read_silver_batch(settings: Settings, entity: str, batch_date: dt.date) -> pd.DataFrame:
    path = settings.silver_root / entity
    frame = pd.read_parquet(path, filters=[("_batch_id", "=", batch_date.isoformat())])
    # Decimal columns arrive as objects; GX numeric comparisons want floats.
    for col in frame.columns:
        if frame[col].dtype == object and len(frame) and hasattr(frame[col].iloc[0], "as_tuple"):
            frame[col] = frame[col].astype(float)
    return frame


def run_quality(settings: Settings, batch_date: dt.date) -> QualityReport:
    report = QualityReport(batch_id=batch_date.isoformat())
    reference = {
        "customers": set(pd.read_parquet(settings.silver_root / "customers", columns=["customer_id"])["customer_id"]),
        "products": set(pd.read_parquet(settings.silver_root / "products", columns=["product_id"])["product_id"]),
    }
    for entity, expectations in suites(reference).items():
        frame = read_silver_batch(settings, entity, batch_date)
        if frame.empty and entity in ("orders", "order_items"):
            logger.info("quality %-12s no new rows in this batch", entity)
            continue
        for check in validate_frame(entity, frame, expectations):
            report.checks.append(check)
            if not check.success:
                msg = f"{entity}.{check.column}: {check.expectation} ({check.unexpected_count} unexpected)"
                if check.severity == "critical":
                    report.errors.append(msg)
                else:
                    logger.warning("quality warning: %s", msg)

    for entity, m in load_silver_metrics(settings, batch_date).items():
        rate = m.quarantined / m.bronze_rows if m.bronze_rows else 0.0
        report.quarantine_rates[entity] = round(rate, 4)
        if rate > settings.max_quarantine_ratio:
            report.errors.append(f"{entity}: quarantine rate {rate:.1%} exceeds {settings.max_quarantine_ratio:.0%}")

    out = settings.metrics_root / f"batch_id={batch_date.isoformat()}" / "quality.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({**asdict(report), "success": report.success}, indent=2))
    if not report.success:
        raise DataQualityError("Data quality gate failed: " + "; ".join(report.errors))
    logger.info("quality passed: %d checks", len(report.checks))
    return report
