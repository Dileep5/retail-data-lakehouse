"""Silver transformations: schema enforcement, normalisation, quarantine, de-duplication."""

import datetime as dt
from decimal import Decimal

import pytest
from pyspark.sql import types as T

from lakehouse.silver import (
    clean_customers,
    clean_inventory,
    clean_order_items,
    clean_orders,
    clean_products,
)


def _df(spark, rows: list[dict]):
    columns = list(rows[0].keys())
    schema = T.StructType([T.StructField(c, T.StringType()) for c in columns])
    return spark.createDataFrame([tuple(r[c] for c in columns) for r in rows], schema)


CUSTOMER = {
    "customer_id": "1", "first_name": "olivia", "last_name": "smith", "email": "olivia@example.com",
    "city": "toronto", "province": "ON", "loyalty_tier": "gold", "signup_date": "2024-01-02",
    "updated_at": "2025-10-01 09:00:00", "_batch_id": "2025-11-01",
}


def test_customers_normalised_and_typed(spark):
    raw = {**CUSTOMER, "email": "  OLIVIA@Example.COM ", "province": "on"}
    valid, rejected = clean_customers(_df(spark, [raw]))
    row = valid.collect()[0]
    assert rejected.count() == 0
    assert row.email == "olivia@example.com"
    assert row.province == "ON"
    assert row.first_name == "Olivia" and row.city == "Toronto" and row.loyalty_tier == "Gold"
    assert row.signup_date == dt.date(2024, 1, 2)
    assert isinstance(row.updated_at, dt.datetime)
    assert dict(valid.dtypes)["customer_id"] == "bigint"


@pytest.mark.parametrize(
    "override, reason",
    [
        ({"customer_id": ""}, "missing_customer_id"),
        ({"customer_id": "abc"}, "missing_customer_id"),
        ({"email": "not-an-email"}, "invalid_email"),
        ({"province": "XX"}, "invalid_province"),
        ({"loyalty_tier": "Platinum"}, "invalid_loyalty_tier"),
        ({"signup_date": "31/12/2024"}, "missing_signup_date"),
    ],
)
def test_customers_quarantine_reasons(spark, override, reason):
    valid, rejected = clean_customers(_df(spark, [{**CUSTOMER, **override}]))
    assert valid.count() == 0
    assert rejected.collect()[0]._reject_reason == reason


def test_customers_dedupe_keeps_latest_version(spark):
    rows = [
        {**CUSTOMER, "loyalty_tier": "Silver", "updated_at": "2025-09-01 00:00:00"},
        {**CUSTOMER, "loyalty_tier": "Gold", "updated_at": "2025-10-01 00:00:00"},
        {**CUSTOMER, "loyalty_tier": "Gold", "updated_at": "2025-10-01 00:00:00"},  # exact duplicate
    ]
    valid, _ = clean_customers(_df(spark, rows))
    out = valid.collect()
    assert len(out) == 1 and out[0].loyalty_tier == "Gold"


ORDER = {"order_id": "10", "customer_id": "1", "store_id": "2", "order_date": "2025-10-31",
         "channel": "online", "status": "completed", "created_at": "2025-10-31 10:00:00",
         "updated_at": "2025-10-31 10:00:00", "_batch_id": "2025-11-01"}


@pytest.mark.parametrize("raw, expected", [(" Completed ", "completed"), ("CANCELED", "cancelled"),
                                           ("complete", "completed"), ("returned", "returned")])
def test_order_status_normalisation(spark, raw, expected):
    valid, rejected = clean_orders(_df(spark, [{**ORDER, "status": raw}]))
    assert rejected.count() == 0
    assert valid.collect()[0].status == expected


def test_order_channel_normalisation_and_month(spark):
    valid, _ = clean_orders(_df(spark, [{**ORDER, "channel": "IN-STORE"}]))
    row = valid.collect()[0]
    assert row.channel == "in_store" and row.order_month == "2025-10"


def test_order_unknown_status_quarantined(spark):
    _, rejected = clean_orders(_df(spark, [{**ORDER, "status": "lost"}]))
    assert rejected.collect()[0]._reject_reason == "invalid_status"


def test_order_late_update_wins(spark):
    rows = [ORDER, {**ORDER, "status": "returned", "updated_at": "2025-11-05 12:00:00"}]
    valid, _ = clean_orders(_df(spark, rows))
    assert [r.status for r in valid.collect()] == ["returned"]


ITEM = {"order_item_id": "100", "order_id": "10", "product_id": "3", "quantity": "2",
        "unit_price": "19.99", "discount_pct": "10", "created_at": "2025-10-31 10:00:00",
        "updated_at": "2025-10-31 10:00:00", "_batch_id": "2025-11-01"}


def test_order_items_decimal_types(spark):
    valid, _ = clean_order_items(_df(spark, [ITEM]))
    row = valid.collect()[0]
    assert row.unit_price == Decimal("19.99") and row.discount_pct == Decimal("10.00")
    assert dict(valid.dtypes)["unit_price"] == "decimal(10,2)"


@pytest.mark.parametrize(
    "override, reason",
    [({"quantity": "0"}, "non_positive_quantity"), ({"quantity": "-1"}, "non_positive_quantity"),
     ({"unit_price": "-5"}, "invalid_unit_price"), ({"discount_pct": "150"}, "invalid_discount"),
     ({"product_id": ""}, "missing_product_id")],
)
def test_order_items_quarantine(spark, override, reason):
    _, rejected = clean_order_items(_df(spark, [{**ITEM, **override}]))
    assert rejected.collect()[0]._reject_reason == reason


def test_missing_discount_defaults_to_zero(spark):
    valid, _ = clean_order_items(_df(spark, [{**ITEM, "discount_pct": ""}]))
    assert valid.collect()[0].discount_pct == Decimal("0.00")


def test_products_negative_price_quarantined(spark):
    raw = {"product_id": "1", "sku": "mr-ten-1", "product_name": "Tent", "category": "Tents", "brand": "",
           "unit_price": "-1", "unit_cost": "10", "is_active": "True", "updated_at": "2025-07-01 08:00:00",
           "_batch_id": "2025-11-01"}
    valid, rejected = clean_products(_df(spark, [raw, {**raw, "product_id": "2", "unit_price": "99.5"}]))
    assert rejected.collect()[0]._reject_reason == "invalid_unit_price"
    row = valid.collect()[0]
    assert row.sku == "MR-TEN-1" and row.brand == "Unknown" and row.is_active is True


def test_inventory_negative_quantity_and_dedupe(spark):
    base = {"snapshot_date": "2025-11-01", "store_id": "1", "product_id": "1", "quantity_on_hand": "5",
            "reorder_point": "10", "_batch_id": "2025-11-01"}
    rows = [base, {**base, "quantity_on_hand": "-5"}, dict(base)]
    valid, rejected = clean_inventory(_df(spark, rows))
    assert valid.count() == 1
    assert rejected.collect()[0]._reject_reason == "negative_quantity"
