import datetime as dt

import pytest

from lakehouse.generate import START_DATE, STORE_OPENED, simulate, write_landing_files

D1, D2, D3 = START_DATE, START_DATE + dt.timedelta(days=1), START_DATE + dt.timedelta(days=2)


def test_generation_is_deterministic(tmp_path):
    a, b = simulate(D2), simulate(D2)
    assert a.orders == b.orders and a.order_items == b.order_items
    assert a.customers.equals(b.customers)
    f1 = write_landing_files(a, tmp_path / "x")
    f2 = write_landing_files(b, tmp_path / "y")
    for name in ("customers.csv", "products.csv", "inventory.csv"):
        assert (f1 / name).read_bytes() == (f2 / name).read_bytes()


def test_ids_are_unique_across_batches():
    orders, items = [], []
    for d in (D1, D2, D3):
        out = simulate(d)
        orders += [o[0] for o in out.orders]
        items += [i[0] for i in out.order_items]
    assert len(orders) == len(set(orders))
    assert len(items) == len(set(items))


def test_incremental_batches_contain_new_orders_and_late_updates():
    out = simulate(D2)
    activity_day = D2 - dt.timedelta(days=1)
    assert out.orders and all(o[3] == activity_day for o in out.orders)
    assert out.status_updates, "expected late status changes on earlier orders"
    assert all(oid < min(o[0] for o in out.orders) for oid, _s, _t in out.status_updates)


def test_customers_change_between_snapshots():
    before = simulate(D1).customers.dropna(subset=["customer_id"]).set_index("customer_id")
    after = simulate(D2).customers.dropna(subset=["customer_id"]).set_index("customer_id")
    common = before.index.intersection(after.index)
    common = common[common < 900_000]
    b = before.loc[common].groupby(level=0).first()
    a = after.loc[common].groupby(level=0).first()
    changed = (b[["loyalty_tier", "city"]] != a[["loyalty_tier", "city"]]).any(axis=1)
    assert changed.sum() >= 5
    assert len(after.index.unique()) > len(before.index.unique())  # new sign-ups


def test_injected_defects_present():
    out = simulate(D1)
    assert out.customers["customer_id"].isna().sum() == 1
    assert (out.customers["email"] == "not-an-email").sum() == 1
    assert out.customers.duplicated().sum() >= 2
    assert any(i[3] == 0 for i in out.order_items)
    assert any(o[5] != o[5].strip().lower() for o in out.orders)
    assert (out.products["unit_price"] < 0).sum() == 1


def test_orders_respect_store_opening_and_signup_dates():
    out = simulate(D1)
    signup = dict(zip(out.customers["customer_id"], out.customers["signup_date"], strict=True))
    for _oid, cid, store, order_date, *_ in out.orders:
        assert order_date >= STORE_OPENED.get(store, dt.date.min)
        assert order_date >= signup[cid]


def test_rejects_dates_before_start():
    with pytest.raises(ValueError):
        simulate(START_DATE - dt.timedelta(days=1))
