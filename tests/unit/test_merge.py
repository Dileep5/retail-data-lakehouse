"""Upsert-merge semantics and idempotency of Parquet writes."""

import datetime as dt

from lakehouse.merge import overwrite_partitions, replace_dataset, upsert_parquet


def _orders(spark, rows):
    return spark.createDataFrame(rows, "order_id long, status string, updated_at timestamp, order_month string")


def _snapshot(spark, path):
    return sorted(tuple(r) for r in spark.read.parquet(str(path)).select(
        "order_id", "status", "updated_at", "order_month").collect())


T1 = dt.datetime(2025, 10, 1, 10)
T2 = dt.datetime(2025, 11, 2, 9)


def test_upsert_inserts_updates_and_is_idempotent(spark, tmp_path):
    target = tmp_path / "orders"
    upsert_parquet(spark, _orders(spark, [(1, "completed", T1, "2025-10"), (2, "completed", T1, "2025-10")]),
                   target, keys=["order_id"], order_by="updated_at", partition_cols=["order_month"])

    changes = _orders(spark, [(2, "returned", T2, "2025-10"), (3, "completed", T2, "2025-11")])
    upsert_parquet(spark, changes, target, keys=["order_id"], order_by="updated_at", partition_cols=["order_month"])
    first = _snapshot(spark, target)
    assert first == [(1, "completed", T1, "2025-10"), (2, "returned", T2, "2025-10"), (3, "completed", T2, "2025-11")]

    # Re-applying the same batch changes nothing.
    upsert_parquet(spark, changes, target, keys=["order_id"], order_by="updated_at", partition_cols=["order_month"])
    assert _snapshot(spark, target) == first


def test_upsert_ignores_stale_updates(spark, tmp_path):
    target = tmp_path / "orders"
    upsert_parquet(spark, _orders(spark, [(1, "returned", T2, "2025-10")]), target,
                   keys=["order_id"], order_by="updated_at", partition_cols=["order_month"])
    upsert_parquet(spark, _orders(spark, [(1, "completed", T1, "2025-10")]), target,
                   keys=["order_id"], order_by="updated_at", partition_cols=["order_month"])
    assert _snapshot(spark, target) == [(1, "returned", T2, "2025-10")]


def test_upsert_only_rewrites_touched_partitions(spark, tmp_path):
    target = tmp_path / "orders"
    upsert_parquet(spark, _orders(spark, [(1, "completed", T1, "2025-09"), (2, "completed", T1, "2025-10")]),
                   target, keys=["order_id"], order_by="updated_at", partition_cols=["order_month"])
    september = target / "order_month=2025-09"
    before = sorted(p.name for p in september.iterdir())

    upsert_parquet(spark, _orders(spark, [(2, "returned", T2, "2025-10")]), target,
                   keys=["order_id"], order_by="updated_at", partition_cols=["order_month"])
    assert sorted(p.name for p in september.iterdir()) == before  # untouched files
    assert not list(tmp_path.glob(".orders_staging_*"))  # staging cleaned up


def test_upsert_unpartitioned(spark, tmp_path):
    target = tmp_path / "items"
    df = spark.createDataFrame([(1, 5, T1)], "order_item_id long, quantity int, updated_at timestamp")
    upsert_parquet(spark, df, target, keys=["order_item_id"], order_by="updated_at")
    df2 = spark.createDataFrame([(1, 7, T2), (2, 1, T2)], "order_item_id long, quantity int, updated_at timestamp")
    upsert_parquet(spark, df2, target, keys=["order_item_id"], order_by="updated_at")
    rows = sorted((r.order_item_id, r.quantity) for r in spark.read.parquet(str(target)).collect())
    assert rows == [(1, 7), (2, 1)]


def test_replace_dataset_and_partition_overwrite(spark, tmp_path):
    target = tmp_path / "customers"
    replace_dataset(spark.createDataFrame([(1,), (2,)], "id int"), target)
    replace_dataset(spark.createDataFrame([(3,)], "id int"), target)
    assert [r.id for r in spark.read.parquet(str(target)).collect()] == [3]

    inv = tmp_path / "inventory"
    d1, d2 = dt.date(2025, 11, 1), dt.date(2025, 11, 2)
    schema = "snapshot_date date, qty int"
    overwrite_partitions(spark.createDataFrame([(d1, 1)], schema), inv, ["snapshot_date"])
    overwrite_partitions(spark.createDataFrame([(d2, 2)], schema), inv, ["snapshot_date"])
    overwrite_partitions(spark.createDataFrame([(d2, 3)], schema), inv, ["snapshot_date"])  # rerun d2
    rows = sorted((r.snapshot_date, r.qty) for r in spark.read.parquet(str(inv)).collect())
    assert rows == [(d1, 1), (d2, 3)]
