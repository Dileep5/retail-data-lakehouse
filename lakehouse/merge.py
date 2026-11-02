"""Idempotent Parquet write patterns for a local / mounted file system.

Spark cannot overwrite a path it is lazily reading from, so merges are materialised
into a staging directory first and then swapped in. On object storage you would use
a table format with ACID MERGE (Delta Lake, Apache Iceberg) instead; the semantics
here are the same: latest version per key wins and re-running a batch is a no-op.
"""

from __future__ import annotations

import shutil
import uuid
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


def _staging_path(target: Path) -> Path:
    return target.parent / f".{target.name}_staging_{uuid.uuid4().hex[:8]}"


def replace_dataset(df: DataFrame, target: Path) -> None:
    """Atomically-ish replace a whole dataset (used for full-snapshot entities)."""
    staging = _staging_path(target)
    df.write.mode("overwrite").parquet(str(staging))
    if target.exists():
        shutil.rmtree(target)
    staging.rename(target)


def overwrite_partitions(df: DataFrame, target: Path, partition_cols: list[str]) -> None:
    """Overwrite only the partitions present in ``df`` (dynamic partition overwrite)."""
    df.write.mode("overwrite").partitionBy(*partition_cols).parquet(str(target))


def _swap_partitions(staging: Path, target: Path) -> None:
    target.mkdir(parents=True, exist_ok=True)
    # Leaf partition directories are the ones holding parquet files. Collect them
    # before moving anything so the directory walk is not mutated mid-iteration.
    leaves = sorted({f.parent for f in staging.rglob("*.parquet")})
    for part in leaves:
        dest = target / part.relative_to(staging)
        if dest.exists():
            shutil.rmtree(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        part.rename(dest)
    shutil.rmtree(staging)


def upsert_parquet(
    spark: SparkSession,
    updates: DataFrame,
    target: Path,
    *,
    keys: list[str],
    order_by: str,
    partition_cols: list[str] | None = None,
) -> None:
    """Merge ``updates`` into the Parquet dataset at ``target`` by business key.

    For each key the row with the greatest ``order_by`` wins; on ties the incoming row
    wins. With ``partition_cols`` only partitions touched by ``updates`` are rewritten.
    """
    partition_cols = partition_cols or []
    staging = _staging_path(target)

    if target.exists():
        existing = spark.read.parquet(str(target))
        if partition_cols:
            touched = updates.select(*partition_cols).distinct()
            existing = existing.join(touched, on=partition_cols, how="left_semi")
        combined = existing.withColumn("_incoming", F.lit(0)).unionByName(
            updates.withColumn("_incoming", F.lit(1)), allowMissingColumns=True
        )
    else:
        combined = updates.withColumn("_incoming", F.lit(1))

    w = Window.partitionBy(*keys).orderBy(F.col(order_by).desc_nulls_last(), F.col("_incoming").desc())
    merged = combined.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn", "_incoming")

    writer = merged.write.mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(str(staging))

    if partition_cols:
        _swap_partitions(staging, target)
    else:
        if target.exists():
            shutil.rmtree(target)
        staging.rename(target)
