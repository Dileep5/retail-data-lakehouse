import datetime as dt

import pandas as pd
import pytest

from lakehouse.bronze import EPOCH, compute_window
from lakehouse.quality import suites, validate_frame

D1, D2, D3 = dt.date(2025, 11, 1), dt.date(2025, 11, 2), dt.date(2025, 11, 3)
MID = dt.datetime.combine


# ------------------------------------------------------------- batch windows
def test_first_batch_extracts_full_history():
    w = compute_window(D1, [])
    assert w.start == EPOCH and w.end == MID(D1, dt.time())


def test_next_batch_starts_where_previous_succeeded_batch_ended():
    prev = [(D1, EPOCH, MID(D1, dt.time()), "succeeded")]
    w = compute_window(D2, prev)
    assert (w.start, w.end) == (MID(D1, dt.time()), MID(D2, dt.time()))


def test_rerun_reuses_original_window():
    prev = [(D1, EPOCH, MID(D1, dt.time()), "succeeded"), (D2, MID(D1, dt.time()), MID(D2, dt.time()), "succeeded")]
    assert compute_window(D2, prev).start == MID(D1, dt.time())


def test_failed_batch_data_is_caught_up_by_next_batch():
    prev = [(D1, EPOCH, MID(D1, dt.time()), "succeeded"), (D2, MID(D1, dt.time()), MID(D2, dt.time()), "failed")]
    assert compute_window(D3, prev).start == MID(D1, dt.time())


def test_window_that_would_go_backwards_is_rejected():
    # An earlier batch already extracted past this batch's cutoff.
    prev = [(D1, EPOCH, MID(D3, dt.time()), "succeeded")]
    with pytest.raises(ValueError):
        compute_window(D2, prev)


# ------------------------------------------------------- great expectations
def _orders(**overrides):
    frame = pd.DataFrame({
        "order_id": [1, 2, 3], "customer_id": [10, 11, 12], "store_id": [1, 2, 8],
        "status": ["completed", "returned", "cancelled"], "channel": ["online", "in_store", "online"],
    })
    for col, values in overrides.items():
        frame[col] = values
    return frame


def _failed(checks):
    return {(c.expectation, c.column, c.severity) for c in checks if not c.success}


def test_quality_suite_passes_clean_orders():
    checks = validate_frame("orders", _orders(), suites({"customers": {10, 11, 12}})["orders"])
    assert checks and all(c.success for c in checks)


def test_quality_suite_flags_critical_failures():
    bad = _orders(order_id=[1, 1, 3], status=["completed", "lost", "cancelled"], store_id=[1, 2, 99])
    failed = _failed(validate_frame("orders", bad, suites({"customers": {10, 11, 12}})["orders"]))
    assert ("expect_column_values_to_be_unique", "order_id", "critical") in failed
    assert ("expect_column_values_to_be_in_set", "status", "critical") in failed
    assert ("expect_column_values_to_be_between", "store_id", "critical") in failed


def test_unknown_customer_is_only_a_warning():
    failed = _failed(validate_frame("orders", _orders(), suites({"customers": {10}})["orders"]))
    assert failed == {("expect_column_values_to_be_in_set", "customer_id", "warning")}


def test_inventory_compound_key_uniqueness():
    frame = pd.DataFrame({"snapshot_date": ["2025-11-01"] * 2, "store_id": [1, 1], "product_id": [5, 5],
                          "quantity_on_hand": [3, 4]})
    failed = _failed(validate_frame("inventory", frame, suites({})["inventory"]))
    assert ("expect_compound_columns_to_be_unique", "snapshot_date,store_id,product_id", "critical") in failed
