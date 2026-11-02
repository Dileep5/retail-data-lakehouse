"""End-to-end pipeline tests against real PostgreSQL (+ Spark, GX, dbt).

    TEST_PG_ADMIN_URL=postgresql://postgres:postgres@localhost:5432/postgres pytest tests/integration -q

Creates and drops two throwaway databases on that server.
"""

import datetime as dt

import pandas as pd
import psycopg
import pytest
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from lakehouse.bronze import OutOfOrderBatchError
from lakehouse.config import Settings
from lakehouse.db import drop_database
from lakehouse.fingerprint import fingerprint
from lakehouse.generate import START_DATE, simulate, write_landing_files
from lakehouse.pipeline import STEP_FUNCTIONS, init, run_batch
from lakehouse.quality import DataQualityError

pytestmark = pytest.mark.postgres

D1, D2, D3 = START_DATE, START_DATE + dt.timedelta(days=1), START_DATE + dt.timedelta(days=2)


def _url(admin: str, dbname: str) -> str:
    return make_conninfo(**{**conninfo_to_dict(admin), "dbname": dbname})


@pytest.fixture(scope="module")
def env(tmp_path_factory):
    import os

    admin = os.environ["TEST_PG_ADMIN_URL"]
    tmp = tmp_path_factory.mktemp("lakehouse")
    settings = Settings(
        _env_file=None,
        lake_root=tmp / "lake",
        landing_root=tmp / "landing",
        source_db_url=_url(admin, "lakehouse_source_test"),
        warehouse_db_url=_url(admin, "lakehouse_dw_test"),
        spark_master="local[2]",
    )
    for url in (settings.source_db_url, settings.warehouse_db_url):
        drop_database(url)
    init(settings)
    results = {D1: run_batch(settings, D1), D2: run_batch(settings, D2)}
    # Snapshot of the warehouse after D2, before the late customer's CRM row arrives.
    results["after_d2"] = {
        "unknown": _q(settings, "SELECT order_id FROM analytics_marts.fct_orders WHERE customer_sk = '-1'"),
    }
    results[D3] = run_batch(settings, D3)
    yield settings, results
    for url in (settings.source_db_url, settings.warehouse_db_url):
        drop_database(url)


def _q(settings, query, *params):
    with psycopg.connect(settings.warehouse_db_url) as conn:
        return conn.execute(query, params).fetchall()


def test_all_steps_succeeded(env):
    settings, results = env
    for batch in (D1, D2, D3):
        assert set(results[batch]) == set(STEP_FUNCTIONS)
        assert results[batch]["reconcile"]["checks_passed"] >= 14
    assert _q(settings, "SELECT batch_id, status FROM meta.ingestion_batches ORDER BY 1") == \
        [(D1, "succeeded"), (D2, "succeeded"), (D3, "succeeded")]


def test_incremental_extract_only_contains_window_changes(env):
    settings, results = env
    assert results[D1]["bronze"]["orders"] > 1000  # full history
    assert 10 < results[D2]["bronze"]["orders"] < 60  # one day of new orders + late status changes
    bronze_d2 = pd.read_parquet(settings.bronze_root / "orders" / f"batch_id={D2}")
    assert (pd.to_datetime(bronze_d2["updated_at"]) >= pd.Timestamp(D1)).all()


def test_late_status_change_updates_silver_and_fact(env):
    settings, _ = env
    bronze_d2 = pd.read_parquet(settings.bronze_root / "orders" / f"batch_id={D2}")
    late = bronze_d2[pd.to_datetime(bronze_d2["order_date"]) < pd.Timestamp(D1)]
    assert not late.empty
    order_id = int(late.iloc[0]["order_id"])
    assert _q(settings, "SELECT status, recognized_revenue FROM analytics_marts.fct_orders WHERE order_id = %s",
              order_id) == [("returned", 0)]


def test_late_arriving_line_item_updates_earlier_order(env):
    settings, _ = env
    late_order = simulate(D2).late_item_order_ids[0]
    fact = _q(settings, "SELECT order_revenue, line_count FROM analytics_marts.fct_orders WHERE order_id = %s",
              late_order)[0]
    staging = _q(settings, "SELECT sum(line_revenue), count(*) FROM analytics_staging.stg_order_items "
                           "WHERE order_id = %s", late_order)[0]
    assert fact == staging  # the order fact includes the item that arrived a batch later


def test_late_arriving_customer_is_reattributed(env):
    settings, results = env
    late_customer = simulate(D2).late_customer_ids[0]
    order_id = _q(settings, "SELECT order_id FROM lake_silver.orders WHERE customer_id = %s", late_customer)[0][0]
    # After D2 the order existed but its customer was unknown (CRM row not exported yet) ...
    assert (order_id,) in results["after_d2"]["unknown"]
    # ... after D3 it is attributed to the customer's SCD2 version.
    row = _q(settings, "SELECT f.customer_sk, f.customer_resolution, d.customer_id FROM analytics_marts.fct_orders f "
                       "JOIN analytics_marts.dim_customers d USING (customer_sk) WHERE f.order_id = %s", order_id)
    assert row == [(row[0][0], "point_in_time", late_customer)]


def test_quarantine_captures_defects(env):
    settings, _ = env
    reasons = set(pd.read_parquet(settings.quarantine_root / "customers" / f"batch_id={D2}")["_reject_reason"])
    assert reasons == {"invalid_email", "missing_customer_id"}
    items = pd.read_parquet(settings.quarantine_root / "order_items" / f"batch_id={D2}")
    assert set(items["_reject_reason"]) == {"non_positive_quantity"}


def test_scd2_history(env):
    settings, _ = env
    rows = _q(settings, """
        SELECT customer_id, count(*), count(*) FILTER (WHERE is_current)
        FROM analytics_marts.dim_customers GROUP BY 1 HAVING count(*) > 1""")
    assert len(rows) >= 10  # customers whose tier or city changed in batches 2-3
    assert all(current == 1 for _cid, _total, current in rows)
    for cid, total, _ in rows:
        versions = _q(settings, "SELECT valid_from, valid_to FROM analytics_marts.dim_customers "
                                "WHERE customer_id = %s ORDER BY valid_from", cid)
        assert len(versions) == total
        # Contiguous history: each version ends exactly when the next begins; the last is open.
        assert all(a[1] == b[0] for a, b in zip(versions, versions[1:], strict=False))
        assert versions[-1][1] is None


def test_point_in_time_customer_attribution(env):
    settings, _ = env
    rows = dict(_q(settings, "SELECT customer_resolution, count(*) FROM analytics_marts.fct_orders GROUP BY 1"))
    assert "unresolved" not in rows and rows.get("unknown", 0) == 1  # only D3's own late customer
    assert _q(settings, "SELECT count(*) FROM analytics_marts.fct_orders WHERE customer_sk IS NULL")[0][0] == 0


def test_rerun_is_idempotent(env):
    settings, _ = env
    before = fingerprint(settings.warehouse_db_url)
    lake_before = sorted(str(p.relative_to(settings.lake_root)) for p in settings.silver_root.rglob("*.parquet"))
    run_batch(settings, D3)
    assert fingerprint(settings.warehouse_db_url) == before
    silver_orders = pd.read_parquet(settings.silver_root / "orders")
    assert silver_orders["order_id"].is_unique
    assert len(lake_before) > 0


def test_rerunning_an_older_batch_is_refused(env):
    settings, _ = env
    before = fingerprint(settings.warehouse_db_url)
    with pytest.raises(OutOfOrderBatchError):
        run_batch(settings, D1)
    assert fingerprint(settings.warehouse_db_url) == before


def test_quality_gate_blocks_bad_batch_and_recovers(env):
    settings, _ = env
    bad_day = D3 + dt.timedelta(days=1)
    STEP_FUNCTIONS["generate"](settings, bad_day)
    # Corrupt the CRM export: 20% of emails invalid -> quarantine rate above the 5% gate.
    path = settings.landing_root / bad_day.isoformat() / "customers.csv"
    frame = pd.read_csv(path)
    frame.loc[frame.sample(frac=0.2, random_state=1).index, "email"] = "broken"
    frame.to_csv(path, index=False)

    before = fingerprint(settings.warehouse_db_url)
    with pytest.raises(DataQualityError, match="quarantine rate"):
        run_batch(settings, bad_day, skip_generate=True)
    assert fingerprint(settings.warehouse_db_url) == before  # nothing loaded
    assert _q(settings, "SELECT status FROM meta.ingestion_batches WHERE batch_id = %s", bad_day) == [("failed",)]

    # Recovery: the source re-delivers a clean file; re-running the same batch succeeds.
    write_landing_files(simulate(bad_day), settings.landing_root)
    result = run_batch(settings, bad_day, skip_generate=True)
    assert result["reconcile"]["checks_passed"] >= 14
    assert _q(settings, "SELECT status FROM meta.ingestion_batches WHERE batch_id = %s", bad_day) == \
        [("succeeded",)]
