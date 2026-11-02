-- Operational (OLTP) source system. Deliberately lenient (few constraints), like many
-- real source systems, so the lakehouse has to clean and validate the data.
CREATE SCHEMA IF NOT EXISTS sales;

CREATE TABLE IF NOT EXISTS sales.orders (
    order_id     BIGINT PRIMARY KEY,
    customer_id  BIGINT,
    store_id     INTEGER,
    order_date   DATE,
    channel      TEXT,
    status       TEXT,
    created_at   TIMESTAMP NOT NULL,
    updated_at   TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS sales.order_items (
    order_item_id BIGINT PRIMARY KEY,
    order_id      BIGINT,
    product_id    INTEGER,
    quantity      INTEGER,
    unit_price    NUMERIC(10, 2),
    discount_pct  NUMERIC(5, 2),
    created_at    TIMESTAMP NOT NULL,
    updated_at    TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_orders_updated_at ON sales.orders (updated_at);
CREATE INDEX IF NOT EXISTS idx_order_items_updated_at ON sales.order_items (updated_at);

-- Records which synthetic batches were generated, so generation is idempotent.
CREATE TABLE IF NOT EXISTS sales.generator_log (
    batch_date   DATE PRIMARY KEY,
    orders       INTEGER NOT NULL,
    order_items  INTEGER NOT NULL,
    status_updates INTEGER NOT NULL,
    generated_at TIMESTAMP NOT NULL DEFAULT now()
);
