"""Silver layer (PySpark): typed, cleaned, validated and de-duplicated data.

Each ``clean_*`` function is a pure DataFrame -> (valid, rejected) transformation:

1. normalise text (trim, case) and map known source variants (" Completed " -> "completed");
2. enforce the target schema with explicit casts (malformed values become NULL);
3. evaluate validation rules; rows failing any rule go to quarantine with a reason;
4. de-duplicate on the business key, keeping the latest ``updated_at``.

``run_silver`` applies them to one Bronze batch and writes Parquet:
snapshot entities (customers, products) are replaced, inventory overwrites its
snapshot_date partition, and orders / order items are upsert-merged by key.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
from dataclasses import asdict, dataclass

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from lakehouse.bronze import partition_path
from lakehouse.config import Settings
from lakehouse.merge import overwrite_partitions, replace_dataset, upsert_parquet

logger = logging.getLogger(__name__)

EMAIL_REGEX = r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"
PROVINCES = ["AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU", "ON", "PE", "QC", "SK", "YT"]
TIERS = ["Bronze", "Silver", "Gold"]
CHANNELS = ["online", "in_store"]
STATUSES = ["completed", "returned", "cancelled"]
STATUS_ALIASES = {"complete": "completed", "return": "returned", "canceled": "cancelled"}

MONEY = T.DecimalType(10, 2)
PCT = T.DecimalType(5, 2)


@dataclass
class EntityMetrics:
    entity: str
    bronze_rows: int
    quarantined: int
    duplicates_removed: int
    silver_rows_written: int

    @property
    def reconciles(self) -> bool:
        return self.bronze_rows == self.quarantined + self.duplicates_removed + self.silver_rows_written


# ------------------------------------------------------------------ helpers
def _blank_to_null(col: str) -> Column:
    trimmed = F.trim(F.col(col))
    return F.when(trimmed == "", None).otherwise(trimmed)


def _apply_rules(df: DataFrame, rules: list[tuple[str, Column]]) -> tuple[DataFrame, DataFrame]:
    """Attach the first failing rule as ``_reject_reason``; split valid vs rejected."""
    reason = F.lit(None).cast("string")
    for name, passes in reversed(rules):
        reason = F.when(~F.coalesce(passes, F.lit(False)), F.lit(name)).otherwise(reason)
    tagged = df.withColumn("_reject_reason", reason)
    valid = tagged.filter(F.col("_reject_reason").isNull()).drop("_reject_reason")
    rejected = tagged.filter(F.col("_reject_reason").isNotNull())
    return valid, rejected


def _dedupe(df: DataFrame, keys: list[str], order_col: str | None = None) -> DataFrame:
    order = [F.col(order_col).desc_nulls_last()] if order_col else []
    # Tie-break on every remaining column so the survivor is deterministic.
    order += [F.col(c).asc_nulls_last() for c in df.columns if c not in keys and c != order_col]
    w = Window.partitionBy(*keys).orderBy(*order)
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def _keep_lineage(df: DataFrame) -> list[str]:
    return [c for c in ("_batch_id",) if c in df.columns]


# ------------------------------------------------------------------ entities
def clean_customers(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    typed = df.select(
        _blank_to_null("customer_id").cast("long").alias("customer_id"),
        F.initcap(_blank_to_null("first_name")).alias("first_name"),
        F.initcap(_blank_to_null("last_name")).alias("last_name"),
        F.lower(_blank_to_null("email")).alias("email"),
        F.initcap(_blank_to_null("city")).alias("city"),
        F.upper(_blank_to_null("province")).alias("province"),
        F.initcap(_blank_to_null("loyalty_tier")).alias("loyalty_tier"),
        F.to_date(_blank_to_null("signup_date")).alias("signup_date"),
        F.to_timestamp(_blank_to_null("updated_at")).alias("updated_at"),
        *_keep_lineage(df),
    )
    valid, rejected = _apply_rules(typed, [
        ("missing_customer_id", F.col("customer_id").isNotNull()),
        ("invalid_email", F.col("email").rlike(EMAIL_REGEX)),
        ("missing_name", F.col("first_name").isNotNull() & F.col("last_name").isNotNull()),
        ("invalid_province", F.col("province").isin(PROVINCES)),
        ("invalid_loyalty_tier", F.col("loyalty_tier").isin(TIERS)),
        ("missing_signup_date", F.col("signup_date").isNotNull()),
        ("missing_updated_at", F.col("updated_at").isNotNull()),
    ])
    return _dedupe(valid, ["customer_id"], "updated_at"), rejected


def clean_products(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    typed = df.select(
        _blank_to_null("product_id").cast("int").alias("product_id"),
        F.upper(_blank_to_null("sku")).alias("sku"),
        _blank_to_null("product_name").alias("product_name"),
        _blank_to_null("category").alias("category"),
        _blank_to_null("brand").alias("brand"),
        _blank_to_null("unit_price").cast(MONEY).alias("unit_price"),
        _blank_to_null("unit_cost").cast(MONEY).alias("unit_cost"),
        F.lower(_blank_to_null("is_active")).isin("true", "1", "yes").alias("is_active"),
        F.to_timestamp(_blank_to_null("updated_at")).alias("updated_at"),
        *_keep_lineage(df),
    )
    valid, rejected = _apply_rules(typed, [
        ("missing_product_id", F.col("product_id").isNotNull()),
        ("missing_sku", F.col("sku").isNotNull()),
        ("missing_category", F.col("category").isNotNull()),
        ("invalid_unit_price", F.col("unit_price") > 0),
        ("invalid_unit_cost", F.col("unit_cost") >= 0),
        ("missing_updated_at", F.col("updated_at").isNotNull()),
    ])
    valid = valid.fillna({"brand": "Unknown", "product_name": "Unknown"})
    return _dedupe(valid, ["product_id"], "updated_at"), rejected


def _normalise_status(col: str) -> Column:
    s = F.lower(_blank_to_null(col))
    mapping = F.create_map(*[F.lit(x) for kv in STATUS_ALIASES.items() for x in kv])
    return F.coalesce(mapping[s], s)


def clean_orders(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    typed = df.select(
        _blank_to_null("order_id").cast("long").alias("order_id"),
        _blank_to_null("customer_id").cast("long").alias("customer_id"),
        _blank_to_null("store_id").cast("int").alias("store_id"),
        F.to_date(_blank_to_null("order_date")).alias("order_date"),
        F.regexp_replace(F.lower(_blank_to_null("channel")), "[- ]", "_").alias("channel"),
        _normalise_status("status").alias("status"),
        F.to_timestamp(_blank_to_null("created_at")).alias("created_at"),
        F.to_timestamp(_blank_to_null("updated_at")).alias("updated_at"),
        *_keep_lineage(df),
    )
    valid, rejected = _apply_rules(typed, [
        ("missing_order_id", F.col("order_id").isNotNull()),
        ("missing_customer_id", F.col("customer_id").isNotNull()),
        ("missing_store_id", F.col("store_id").isNotNull()),
        ("missing_order_date", F.col("order_date").isNotNull()),
        ("invalid_channel", F.col("channel").isin(CHANNELS)),
        ("invalid_status", F.col("status").isin(STATUSES)),
        ("missing_updated_at", F.col("updated_at").isNotNull()),
    ])
    valid = valid.withColumn("order_month", F.date_format("order_date", "yyyy-MM"))
    return _dedupe(valid, ["order_id"], "updated_at"), rejected


def clean_order_items(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    typed = df.select(
        _blank_to_null("order_item_id").cast("long").alias("order_item_id"),
        _blank_to_null("order_id").cast("long").alias("order_id"),
        _blank_to_null("product_id").cast("int").alias("product_id"),
        _blank_to_null("quantity").cast("int").alias("quantity"),
        _blank_to_null("unit_price").cast(MONEY).alias("unit_price"),
        F.coalesce(_blank_to_null("discount_pct").cast(PCT), F.lit(0).cast(PCT)).alias("discount_pct"),
        F.to_timestamp(_blank_to_null("created_at")).alias("created_at"),
        F.to_timestamp(_blank_to_null("updated_at")).alias("updated_at"),
        *_keep_lineage(df),
    )
    valid, rejected = _apply_rules(typed, [
        ("missing_order_item_id", F.col("order_item_id").isNotNull()),
        ("missing_order_id", F.col("order_id").isNotNull()),
        ("missing_product_id", F.col("product_id").isNotNull()),
        ("non_positive_quantity", F.col("quantity") > 0),
        ("invalid_unit_price", F.col("unit_price") >= 0),
        ("invalid_discount", F.col("discount_pct").between(0, 100)),
        ("missing_updated_at", F.col("updated_at").isNotNull()),
    ])
    return _dedupe(valid, ["order_item_id"], "updated_at"), rejected


def clean_inventory(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    typed = df.select(
        F.to_date(_blank_to_null("snapshot_date")).alias("snapshot_date"),
        _blank_to_null("store_id").cast("int").alias("store_id"),
        _blank_to_null("product_id").cast("int").alias("product_id"),
        _blank_to_null("quantity_on_hand").cast("int").alias("quantity_on_hand"),
        _blank_to_null("reorder_point").cast("int").alias("reorder_point"),
        *_keep_lineage(df),
    )
    valid, rejected = _apply_rules(typed, [
        ("missing_key", F.col("snapshot_date").isNotNull() & F.col("store_id").isNotNull()
         & F.col("product_id").isNotNull()),
        ("negative_quantity", F.col("quantity_on_hand") >= 0),
        ("negative_reorder_point", F.col("reorder_point") >= 0),
    ])
    return _dedupe(valid, ["snapshot_date", "store_id", "product_id"]), rejected


CLEANERS = {
    "customers": clean_customers,
    "products": clean_products,
    "inventory": clean_inventory,
    "orders": clean_orders,
    "order_items": clean_order_items,
}


# ------------------------------------------------------------------ runner
def read_bronze(spark: SparkSession, settings: Settings, entity: str, batch_date: dt.date) -> DataFrame:
    path = partition_path(settings.bronze_root, entity, batch_date)
    if not path.exists():
        raise FileNotFoundError(f"Bronze partition missing: {path}")
    return spark.read.parquet(str(path)).drop("_source")


def process_entity(spark: SparkSession, settings: Settings, entity: str, batch_date: dt.date) -> EntityMetrics:
    bronze = read_bronze(spark, settings, entity, batch_date).cache()
    bronze_rows = bronze.count()
    valid, rejected = CLEANERS[entity](bronze)
    valid = valid.cache()
    rejected = rejected.cache()
    quarantined = rejected.count()
    written = valid.count()

    quarantine_path = partition_path(settings.quarantine_root, entity, batch_date)
    rejected.write.mode("overwrite").parquet(str(quarantine_path))

    target = settings.silver_root / entity
    if entity in ("customers", "products"):
        replace_dataset(valid, target)
    elif entity == "inventory":
        overwrite_partitions(valid, target, ["snapshot_date"])
    elif entity == "orders":
        upsert_parquet(spark, valid, target, keys=["order_id"], order_by="updated_at",
                       partition_cols=["order_month"])
    else:
        upsert_parquet(spark, valid, target, keys=["order_item_id"], order_by="updated_at")

    metrics = EntityMetrics(entity, bronze_rows, quarantined, bronze_rows - quarantined - written, written)
    bronze.unpersist()
    valid.unpersist()
    rejected.unpersist()
    logger.info("silver %-12s %s", entity, metrics)
    return metrics


def run_silver(spark: SparkSession, settings: Settings, batch_date: dt.date) -> dict[str, EntityMetrics]:
    metrics = {entity: process_entity(spark, settings, entity, batch_date) for entity in CLEANERS}
    out = settings.metrics_root / f"batch_id={batch_date.isoformat()}"
    out.mkdir(parents=True, exist_ok=True)
    (out / "silver.json").write_text(json.dumps({k: asdict(v) for k, v in metrics.items()}, indent=2))
    return metrics


def load_silver_metrics(settings: Settings, batch_date: dt.date) -> dict[str, EntityMetrics]:
    path = settings.metrics_root / f"batch_id={batch_date.isoformat()}" / "silver.json"
    return {k: EntityMetrics(**v) for k, v in json.loads(path.read_text()).items()}
