"""Deterministic synthetic retail data.

Each pipeline batch date D represents "everything that happened up to the end of D-1":

* CSV exports (full snapshots) land in ``data/landing/<D>/``: customers, products, inventory.
* Orders and order items are written to the OLTP source database (``sales`` schema)
  with ``created_at``/``updated_at`` timestamps, including late status changes
  (completed -> returned) that the incremental extract must pick up.

The simulation replays every day from ``START_DATE`` up to D with a per-day seed, so
generating a batch is a pure function of its date: re-running produces identical files
and rows. Each batch also injects realistic defects (duplicates, bad emails, negative
quantities, inconsistent casing) for the Silver layer to clean or quarantine.
"""

from __future__ import annotations

import datetime as dt
import random
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
import psycopg

START_DATE = dt.date(2025, 11, 1)
HISTORY_START = dt.date(2025, 8, 1)
SEED = 20251101

STORES = {1: ("Toronto", "ON"), 2: ("Ottawa", "ON"), 3: ("Montreal", "QC"), 4: ("Vancouver", "BC"),
          5: ("Calgary", "AB"), 6: ("Waterloo", "ON"), 7: ("Kelowna", "BC"), 8: ("Halifax", "NS")}
CATEGORIES = {
    "Tents": (180, 650), "Sleeping Bags": (60, 320), "Backpacks": (45, 340), "Footwear": (55, 260),
    "Apparel": (35, 420), "Electronics": (30, 480), "Cooking": (20, 160), "Climbing": (25, 380),
}
BRANDS = ["Maple Ridge", "Northbound", "Cedar & Co", "PeakForm", "Lakeshore"]
ADJECTIVES = ["Trail", "Summit", "Alpine", "Boreal", "Glacier", "Canyon", "Ridge", "Tundra"]
FIRST = ["Olivia", "Liam", "Emma", "Noah", "Ava", "Ethan", "Sophia", "Lucas", "Mia", "Aarav", "Chloe",
         "Mohammed", "Zoe", "Wei", "Priya", "Jacob", "Fatima", "Leo", "Hannah", "Arjun"]
LAST = ["Tremblay", "Smith", "Nguyen", "Patel", "Martin", "Roy", "Wilson", "Chen", "Singh", "Brown",
        "Gagnon", "Lee", "Taylor", "Kumar", "Campbell", "Ahmed", "Anderson", "Leblanc", "Wong", "Clark"]
TIERS = ["Bronze", "Silver", "Gold"]
# Must agree with dbt/seeds/stores.csv: no store takes orders before it opens.
STORE_OPENED = {7: dt.date(2025, 3, 14), 8: dt.date(2025, 10, 2)}

CUSTOMER_COLUMNS = ["customer_id", "first_name", "last_name", "email", "city", "province",
                    "loyalty_tier", "signup_date", "updated_at"]
PRODUCT_COLUMNS = ["product_id", "sku", "product_name", "category", "brand", "unit_price", "unit_cost",
                   "is_active", "updated_at"]
INVENTORY_COLUMNS = ["snapshot_date", "store_id", "product_id", "quantity_on_hand", "reorder_point"]


@dataclass
class DayOutput:
    batch_date: dt.date
    customers: pd.DataFrame
    products: pd.DataFrame
    inventory: pd.DataFrame
    orders: list[tuple] = field(default_factory=list)
    order_items: list[tuple] = field(default_factory=list)
    status_updates: list[tuple] = field(default_factory=list)  # (order_id, new_status, updated_at)


@dataclass
class _State:
    customers: dict[int, dict] = field(default_factory=dict)
    products: dict[int, dict] = field(default_factory=dict)
    orders: dict[int, dict] = field(default_factory=dict)  # order_id -> {date, status}
    next_order_id: int = 1
    next_item_id: int = 1
    next_customer_id: int = 1


def _ts(day: dt.date, rng: random.Random, start_hour: int = 8, end_hour: int = 21) -> dt.datetime:
    return dt.datetime.combine(day, dt.time(rng.randint(start_hour, end_hour - 1), rng.randint(0, 59),
                                            rng.randint(0, 59)))


def _new_customer(state: _State, rng: random.Random, signup: dt.date) -> dict:
    cid = state.next_customer_id
    state.next_customer_id += 1
    first, last = rng.choice(FIRST), rng.choice(LAST)
    store = rng.choice(list(STORES.values()))
    customer = {
        "customer_id": cid, "first_name": first, "last_name": last,
        "email": f"{first}.{last}.{cid}@example.com".lower(), "city": store[0], "province": store[1],
        "loyalty_tier": rng.choices(TIERS, weights=[6, 3, 1])[0], "signup_date": signup,
        "updated_at": dt.datetime.combine(signup, dt.time(9, 0)),
    }
    state.customers[cid] = customer
    return customer


def _add_orders(state: _State, rng: random.Random, day: dt.date, count: int, out: DayOutput) -> None:
    eligible = [c for c in state.customers.values() if c["signup_date"] <= day]
    product_ids = sorted(state.products)
    open_stores = [sid for sid in STORES if STORE_OPENED.get(sid, dt.date.min) <= day]
    for _ in range(count):
        oid = state.next_order_id
        state.next_order_id += 1
        customer = rng.choice(eligible)
        created = _ts(day, rng)
        status = rng.choices(["completed", "cancelled"], weights=[95, 5])[0]
        channel = rng.choices(["online", "in_store"], weights=[32, 68])[0]
        state.orders[oid] = {"date": day, "status": status}
        out.orders.append((oid, customer["customer_id"], rng.choice(open_stores), day, channel, status,
                           created, created))
        for pid in rng.sample(product_ids, rng.randint(1, 4)):
            price = state.products[pid]["unit_price"]
            out.order_items.append((state.next_item_id, oid, pid, rng.randint(1, 3), price,
                                    rng.choice([0, 0, 0, 5, 10, 15, 20]), created, created))
            state.next_item_id += 1


def _simulate_day(state: _State, index: int) -> DayOutput:
    rng = random.Random(f"{SEED}-{index}")
    batch_date = START_DATE + dt.timedelta(days=index)
    activity_day = batch_date - dt.timedelta(days=1)
    out = DayOutput(batch_date, pd.DataFrame(), pd.DataFrame(), pd.DataFrame())

    if index == 0:
        # Initial history: products, customers and ~3 months of orders.
        pid = 1
        for category, (low, high) in CATEGORIES.items():
            for _ in range(5):
                price = round(rng.uniform(low, high), 2)
                state.products[pid] = {
                    "product_id": pid, "sku": f"MR-{category[:3].upper()}-{pid:04d}",
                    "product_name": f"{rng.choice(ADJECTIVES)} {category.rstrip('s')} {pid}",
                    "category": category, "brand": rng.choice(BRANDS), "unit_price": price,
                    "unit_cost": round(price * rng.uniform(0.45, 0.65), 2), "is_active": True,
                    "updated_at": dt.datetime(2025, 7, 1, 8, 0),
                }
                pid += 1
        for _ in range(300):
            _new_customer(state, rng, HISTORY_START - dt.timedelta(days=rng.randint(0, 900)))
        day = HISTORY_START
        while day < batch_date:
            _add_orders(state, rng, day, rng.randint(8, 18), out)
            day += dt.timedelta(days=1)
        # Some historical orders were later returned; the latest version is what we extract.
        for oid in rng.sample(sorted(state.orders), 60):
            order = state.orders[oid]
            if order["status"] == "completed":
                order["status"] = "returned"
                updated = _ts(min(order["date"] + dt.timedelta(days=rng.randint(1, 20)), activity_day), rng)
                out.status_updates.append((oid, "returned", updated))
    else:
        # Customer attribute changes drive SCD Type 2 history.
        for cid in rng.sample(sorted(state.customers), 12):
            c = state.customers[cid]
            if rng.random() < 0.5:
                c["loyalty_tier"] = TIERS[min(TIERS.index(c["loyalty_tier"]) + 1, 2)] \
                    if c["loyalty_tier"] != "Gold" else "Silver"
            else:
                city, province = rng.choice([v for v in STORES.values() if v[0] != c["city"]])
                c["city"], c["province"] = city, province
            c["updated_at"] = dt.datetime.combine(activity_day, dt.time(9, 30))
        for _ in range(rng.randint(3, 6)):
            _new_customer(state, rng, activity_day)
        # Product price changes (SCD Type 1 in the warehouse).
        for pid in rng.sample(sorted(state.products), 2):
            p = state.products[pid]
            p["unit_price"] = round(p["unit_price"] * rng.uniform(0.9, 1.1), 2)
            p["updated_at"] = dt.datetime.combine(activity_day, dt.time(7, 0))
        _add_orders(state, rng, activity_day, rng.randint(15, 25), out)
        recent = [oid for oid, o in state.orders.items()
                  if o["status"] == "completed" and (activity_day - o["date"]).days in range(1, 30)]
        for oid in rng.sample(recent, min(4, len(recent))):
            state.orders[oid]["status"] = "returned"
            out.status_updates.append((oid, "returned", _ts(activity_day, rng)))

    _inject_order_defects(state, out, rng)
    out.customers = _customer_snapshot(state, rng, index)
    out.products = _product_snapshot(state, index)
    out.inventory = _inventory_snapshot(state, rng, batch_date)
    return out


def _inject_order_defects(state: _State, out: DayOutput, rng: random.Random) -> None:
    if not out.orders:
        return
    # Inconsistent casing / whitespace from the source application.
    i = rng.randrange(len(out.orders))
    o = list(out.orders[i])
    o[5] = f" {o[5].title()} "
    out.orders[i] = tuple(o)
    j = rng.randrange(len(out.orders))
    o = list(out.orders[j])
    o[4] = o[4].upper()
    out.orders[j] = tuple(o)
    # An invalid line item (zero quantity) that Silver must quarantine.
    bad = list(out.order_items[-1])
    bad[0] = state.next_item_id
    bad[3] = 0
    state.next_item_id += 1
    out.order_items.append(tuple(bad))


def _customer_snapshot(state: _State, rng: random.Random, index: int) -> pd.DataFrame:
    rows = [dict(c) for c in state.customers.values()]
    # Normalisable defects: mixed-case/whitespace emails, lower-case province codes.
    for row in rng.sample(rows, 5):
        row["email"] = f"  {row['email'].upper()} "
    for row in rng.sample(rows, 3):
        row["province"] = row["province"].lower()
    # Exact duplicates (export bug) and garbage rows to quarantine.
    rows += [dict(r) for r in rng.sample(rows, 2)]
    rows.append({**rows[0], "customer_id": 900_000 + index, "email": "not-an-email"})
    rows.append({**rows[1], "customer_id": None})
    frame = pd.DataFrame(rows, columns=CUSTOMER_COLUMNS)
    frame["customer_id"] = frame["customer_id"].astype("Int64")
    return frame


def _product_snapshot(state: _State, index: int) -> pd.DataFrame:
    rows = [dict(p) for p in state.products.values()]
    rows.append({**rows[0], "product_id": 9_000 + index, "sku": f"BAD-{index}", "unit_price": -1.0})
    return pd.DataFrame(rows, columns=PRODUCT_COLUMNS)


def _inventory_snapshot(state: _State, rng: random.Random, snapshot_date: dt.date) -> pd.DataFrame:
    rows = [(snapshot_date, store, pid, rng.randint(0, 60), rng.randint(5, 15))
            for store in STORES for pid in sorted(state.products)]
    rows.append((snapshot_date, 1, 1, -5, 10))  # negative stock count -> quarantine (dup key too)
    return pd.DataFrame(rows, columns=INVENTORY_COLUMNS)


def simulate(batch_date: dt.date) -> DayOutput:
    """Replay the simulation from START_DATE through ``batch_date``."""
    if batch_date < START_DATE:
        raise ValueError(f"batch_date must be on or after {START_DATE}")
    state = _State()
    out = None
    for index in range((batch_date - START_DATE).days + 1):
        out = _simulate_day(state, index)
    return out


# ------------------------------------------------------------------ writers
def write_landing_files(out: DayOutput, landing_root: Path) -> Path:
    folder = landing_root / out.batch_date.isoformat()
    folder.mkdir(parents=True, exist_ok=True)
    out.customers.to_csv(folder / "customers.csv", index=False)
    out.products.to_csv(folder / "products.csv", index=False)
    out.inventory.to_csv(folder / "inventory.csv", index=False)
    return folder


def write_source_db(out: DayOutput, source_url: str) -> bool:
    """Insert this batch's OLTP changes. Returns False if the batch was already generated."""
    with psycopg.connect(source_url) as conn, conn.cursor() as cur:
        cur.execute("SELECT 1 FROM sales.generator_log WHERE batch_date = %s", (out.batch_date,))
        if cur.fetchone():
            return False
        cur.executemany(
            "INSERT INTO sales.orders VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (order_id) DO NOTHING",
            out.orders,
        )
        cur.executemany(
            "INSERT INTO sales.order_items VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (order_item_id) DO NOTHING",
            out.order_items,
        )
        cur.executemany(
            "UPDATE sales.orders SET status = %s, updated_at = %s WHERE order_id = %s",
            [(status, ts, oid) for oid, status, ts in out.status_updates],
        )
        cur.execute(
            "INSERT INTO sales.generator_log (batch_date, orders, order_items, status_updates) VALUES (%s,%s,%s,%s)",
            (out.batch_date, len(out.orders), len(out.order_items), len(out.status_updates)),
        )
        conn.commit()
    return True
