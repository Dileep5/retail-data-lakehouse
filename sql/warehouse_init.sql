-- Warehouse landing schema for Silver data plus pipeline metadata.
-- dbt builds analytics_* schemas and the snapshots schema on top of lake_silver.
CREATE SCHEMA IF NOT EXISTS lake_silver;
CREATE SCHEMA IF NOT EXISTS meta;

CREATE TABLE IF NOT EXISTS lake_silver.customers (
    customer_id   BIGINT PRIMARY KEY,
    first_name    TEXT NOT NULL,
    last_name     TEXT NOT NULL,
    email         TEXT NOT NULL,
    city          TEXT NOT NULL,
    province      TEXT NOT NULL,
    loyalty_tier  TEXT NOT NULL,
    signup_date   DATE NOT NULL,
    updated_at    TIMESTAMP NOT NULL,
    _batch_id     DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS lake_silver.products (
    product_id    INTEGER PRIMARY KEY,
    sku           TEXT NOT NULL,
    product_name  TEXT NOT NULL,
    category      TEXT NOT NULL,
    brand         TEXT NOT NULL,
    unit_price    NUMERIC(10, 2) NOT NULL,
    unit_cost     NUMERIC(10, 2) NOT NULL,
    is_active     BOOLEAN NOT NULL,
    updated_at    TIMESTAMP NOT NULL,
    _batch_id     DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS lake_silver.orders (
    order_id      BIGINT PRIMARY KEY,
    customer_id   BIGINT NOT NULL,
    store_id      INTEGER NOT NULL,
    order_date    DATE NOT NULL,
    channel       TEXT NOT NULL,
    status        TEXT NOT NULL,
    created_at    TIMESTAMP NOT NULL,
    updated_at    TIMESTAMP NOT NULL,
    _batch_id     DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS lake_silver.order_items (
    order_item_id BIGINT PRIMARY KEY,
    order_id      BIGINT NOT NULL,
    product_id    INTEGER NOT NULL,
    quantity      INTEGER NOT NULL,
    unit_price    NUMERIC(10, 2) NOT NULL,
    discount_pct  NUMERIC(5, 2) NOT NULL,
    created_at    TIMESTAMP NOT NULL,
    updated_at    TIMESTAMP NOT NULL,
    _batch_id     DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS lake_silver.inventory (
    snapshot_date     DATE NOT NULL,
    store_id          INTEGER NOT NULL,
    product_id        INTEGER NOT NULL,
    quantity_on_hand  INTEGER NOT NULL,
    reorder_point     INTEGER NOT NULL,
    _batch_id         DATE NOT NULL,
    PRIMARY KEY (snapshot_date, store_id, product_id)
);

-- One row per pipeline batch. The extraction window makes incremental pulls from
-- the OLTP source deterministic, so re-running a batch extracts the same rows.
CREATE TABLE IF NOT EXISTS meta.ingestion_batches (
    batch_id      DATE PRIMARY KEY,
    window_start  TIMESTAMP NOT NULL,
    window_end    TIMESTAMP NOT NULL,
    status        TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    started_at    TIMESTAMP NOT NULL DEFAULT now(),
    finished_at   TIMESTAMP,
    metrics       JSONB
);
