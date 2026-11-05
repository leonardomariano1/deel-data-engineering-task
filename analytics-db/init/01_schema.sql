-- =============================================================================
-- ACME Delivery Services - analytical database
--
-- Layers
--   stage : landing tables for one Spark micro-batch (typed CDC events, truncated per batch)
--   core  : dimensional model (SCD2 dimensions, accumulating-snapshot facts,
--           transactional status-history fact)
--   mart  : serving layer, incrementally maintained aggregates + business views
--   ops   : pipeline observability (batch log, Spark progress, dead letters)
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS ops;

-- -----------------------------------------------------------------------------
-- stage: one row per CDC event (all versions kept, Postgres does the ordering)
-- -----------------------------------------------------------------------------
CREATE UNLOGGED TABLE stage.customers_cdc (
    customer_id      BIGINT NOT NULL,
    customer_name    TEXT,
    customer_address TEXT,
    is_active        BOOLEAN,
    created_at       TIMESTAMP,
    updated_at       TIMESTAMP,
    op               TEXT NOT NULL,
    is_deleted       BOOLEAN NOT NULL,
    source_lsn       BIGINT NOT NULL,
    source_ts        TIMESTAMP NOT NULL,
    kafka_partition  INT NOT NULL,
    kafka_offset     BIGINT NOT NULL
);

CREATE UNLOGGED TABLE stage.products_cdc (
    product_id       BIGINT NOT NULL,
    product_name     TEXT,
    barcode          TEXT,
    unity_price      NUMERIC(18, 4),
    is_active        BOOLEAN,
    created_at       TIMESTAMP,
    updated_at       TIMESTAMP,
    op               TEXT NOT NULL,
    is_deleted       BOOLEAN NOT NULL,
    source_lsn       BIGINT NOT NULL,
    source_ts        TIMESTAMP NOT NULL,
    kafka_partition  INT NOT NULL,
    kafka_offset     BIGINT NOT NULL
);

CREATE UNLOGGED TABLE stage.orders_cdc (
    order_id         BIGINT NOT NULL,
    order_date       DATE,
    delivery_date    DATE,
    customer_id      BIGINT,
    status           TEXT,
    before_status    TEXT,
    created_at       TIMESTAMP,
    updated_at       TIMESTAMP,
    op               TEXT NOT NULL,
    is_deleted       BOOLEAN NOT NULL,
    source_lsn       BIGINT NOT NULL,
    source_ts        TIMESTAMP NOT NULL,
    kafka_partition  INT NOT NULL,
    kafka_offset     BIGINT NOT NULL
);

CREATE UNLOGGED TABLE stage.order_items_cdc (
    order_item_id    BIGINT NOT NULL,
    order_id         BIGINT,
    product_id       BIGINT,
    quantity         INT,
    created_at       TIMESTAMP,
    updated_at       TIMESTAMP,
    op               TEXT NOT NULL,
    is_deleted       BOOLEAN NOT NULL,
    source_lsn       BIGINT NOT NULL,
    source_ts        TIMESTAMP NOT NULL,
    kafka_partition  INT NOT NULL,
    kafka_offset     BIGINT NOT NULL
);

-- -----------------------------------------------------------------------------
-- core: conformed dimensions
-- -----------------------------------------------------------------------------
CREATE TABLE core.dim_date (
    date_key      INT PRIMARY KEY,           -- yyyymmdd
    full_date     DATE NOT NULL UNIQUE,
    year          SMALLINT NOT NULL,
    quarter       SMALLINT NOT NULL,
    month         SMALLINT NOT NULL,
    month_name    TEXT NOT NULL,
    day_of_month  SMALLINT NOT NULL,
    day_of_week   SMALLINT NOT NULL,         -- ISO, 1 = Monday
    day_name      TEXT NOT NULL,
    iso_week      SMALLINT NOT NULL,
    is_weekend    BOOLEAN NOT NULL
);

INSERT INTO core.dim_date
SELECT to_char(d, 'YYYYMMDD')::INT,
       d::DATE,
       EXTRACT(YEAR FROM d),
       EXTRACT(QUARTER FROM d),
       EXTRACT(MONTH FROM d),
       trim(to_char(d, 'Month')),
       EXTRACT(DAY FROM d),
       EXTRACT(ISODOW FROM d),
       trim(to_char(d, 'Day')),
       EXTRACT(WEEK FROM d),
       EXTRACT(ISODOW FROM d) IN (6, 7)
FROM generate_series(DATE '2020-01-01', DATE '2035-12-31', INTERVAL '1 day') AS d;

-- Business definition of "open" lives in data, not in code.
CREATE TABLE core.dim_order_status (
    status       TEXT PRIMARY KEY,
    is_open      BOOLEAN NOT NULL,
    description  TEXT NOT NULL
);

INSERT INTO core.dim_order_status VALUES
    ('PENDING',      TRUE,  'Order created, not yet picked up by the warehouse'),
    ('PROCESSING',   TRUE,  'Order being prepared'),
    ('REPROCESSING', TRUE,  'Order sent back for preparation'),
    ('COMPLETED',    FALSE, 'Order delivered, no further work');

-- SCD type 2: every source version is kept; valid_to / is_current are derived.
CREATE TABLE core.dim_customer (
    customer_sk      BIGSERIAL PRIMARY KEY,
    customer_id      BIGINT NOT NULL,        -- durable (natural) key
    customer_name    TEXT,
    customer_address TEXT,
    is_active        BOOLEAN,
    valid_from       TIMESTAMP NOT NULL,
    valid_to         TIMESTAMP,
    is_current       BOOLEAN NOT NULL DEFAULT FALSE,
    is_deleted       BOOLEAN NOT NULL DEFAULT FALSE,
    source_lsn       BIGINT NOT NULL,
    kafka_offset     BIGINT NOT NULL,
    loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (customer_id, source_lsn, kafka_offset)
);
CREATE UNIQUE INDEX dim_customer_current_ux ON core.dim_customer (customer_id) WHERE is_current;
CREATE INDEX dim_customer_open_version_ix ON core.dim_customer (customer_id) WHERE valid_to IS NULL;

CREATE TABLE core.dim_product (
    product_sk       BIGSERIAL PRIMARY KEY,
    product_id       BIGINT NOT NULL,
    product_name     TEXT,
    barcode          TEXT,
    unity_price      NUMERIC(18, 4),
    is_active        BOOLEAN,
    valid_from       TIMESTAMP NOT NULL,
    valid_to         TIMESTAMP,
    is_current       BOOLEAN NOT NULL DEFAULT FALSE,
    is_deleted       BOOLEAN NOT NULL DEFAULT FALSE,
    source_lsn       BIGINT NOT NULL,
    kafka_offset     BIGINT NOT NULL,
    loaded_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (product_id, source_lsn, kafka_offset)
);
CREATE UNIQUE INDEX dim_product_current_ux ON core.dim_product (product_id) WHERE is_current;
CREATE INDEX dim_product_open_version_ix ON core.dim_product (product_id) WHERE valid_to IS NULL;

-- -----------------------------------------------------------------------------
-- core: facts
-- -----------------------------------------------------------------------------
-- Accumulating snapshot: one row per order, always the latest known state.
CREATE TABLE core.fact_orders (
    order_id           BIGINT PRIMARY KEY,
    customer_id        BIGINT,
    order_date         DATE,
    delivery_date      DATE,
    order_date_key     INT GENERATED ALWAYS AS ((EXTRACT(YEAR FROM order_date) * 10000 + EXTRACT(MONTH FROM order_date) * 100 + EXTRACT(DAY FROM order_date))::INT) STORED,
    delivery_date_key  INT GENERATED ALWAYS AS ((EXTRACT(YEAR FROM delivery_date) * 10000 + EXTRACT(MONTH FROM delivery_date) * 100 + EXTRACT(DAY FROM delivery_date))::INT) STORED,
    status             TEXT,
    created_at         TIMESTAMP,
    updated_at         TIMESTAMP,
    is_deleted         BOOLEAN NOT NULL DEFAULT FALSE,
    source_lsn         BIGINT NOT NULL,
    kafka_offset       BIGINT NOT NULL,
    loaded_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX fact_orders_customer_ix ON core.fact_orders (customer_id);
CREATE INDEX fact_orders_delivery_ix ON core.fact_orders (delivery_date, status);

CREATE TABLE core.fact_order_items (
    order_item_id      BIGINT PRIMARY KEY,
    order_id           BIGINT,
    product_id         BIGINT,
    quantity           INT,
    created_at         TIMESTAMP,
    updated_at         TIMESTAMP,
    is_deleted         BOOLEAN NOT NULL DEFAULT FALSE,
    source_lsn         BIGINT NOT NULL,
    kafka_offset       BIGINT NOT NULL,
    loaded_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX fact_order_items_order_ix ON core.fact_order_items (order_id);
CREATE INDEX fact_order_items_product_ix ON core.fact_order_items (product_id);

-- Transactional fact: every status transition ever observed (the history).
CREATE TABLE core.fact_order_status_events (
    event_id           BIGSERIAL PRIMARY KEY,
    order_id           BIGINT NOT NULL,
    customer_id        BIGINT,
    delivery_date      DATE,
    previous_status    TEXT,
    status             TEXT,
    event_type         TEXT NOT NULL,        -- snapshot | insert | update | delete
    event_ts           TIMESTAMP NOT NULL,   -- business time (updated_at in source)
    source_lsn         BIGINT NOT NULL,
    kafka_partition    INT NOT NULL,
    kafka_offset       BIGINT NOT NULL,
    loaded_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (kafka_partition, kafka_offset)
);
CREATE INDEX fact_order_status_events_order_ix ON core.fact_order_status_events (order_id, event_ts);
CREATE INDEX fact_order_status_events_ts_ix ON core.fact_order_status_events (event_ts);

-- Current order state with the business rule applied once.
CREATE VIEW core.v_order_state AS
SELECT o.*,
       (NOT o.is_deleted AND COALESCE(s.is_open, TRUE)) AS is_open
FROM core.fact_orders o
LEFT JOIN core.dim_order_status s USING (status);

-- -----------------------------------------------------------------------------
-- mart: aggregates maintained incrementally by the streaming job (deltas)
-- -----------------------------------------------------------------------------
CREATE TABLE mart.open_orders_by_delivery_date_status (
    delivery_date  DATE NOT NULL,
    status         TEXT NOT NULL,
    open_orders    BIGINT NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (delivery_date, status)
);

CREATE TABLE mart.pending_items_by_product (
    product_id        BIGINT PRIMARY KEY,
    pending_quantity  BIGINT NOT NULL,
    pending_lines     BIGINT NOT NULL,
    updated_at        TIMESTAMPTZ NOT NULL
);

CREATE TABLE mart.pending_orders_by_customer (
    customer_id            BIGINT PRIMARY KEY,
    open_orders            BIGINT NOT NULL,
    orders_status_pending  BIGINT NOT NULL,
    updated_at             TIMESTAMPTZ NOT NULL
);

-- Business question 1
CREATE VIEW mart.v_open_orders_by_delivery_date_status AS
SELECT a.delivery_date, d.day_name, a.status, a.open_orders, a.updated_at
FROM mart.open_orders_by_delivery_date_status a
LEFT JOIN core.dim_date d ON d.full_date = a.delivery_date;

-- Business question 2 (RANK keeps ties instead of hiding them)
CREATE VIEW mart.v_top3_delivery_dates AS
SELECT delivery_date, open_orders, rnk
FROM (
    SELECT delivery_date,
           SUM(open_orders) AS open_orders,
           RANK() OVER (ORDER BY SUM(open_orders) DESC) AS rnk
    FROM mart.open_orders_by_delivery_date_status
    GROUP BY delivery_date
) t
WHERE rnk <= 3;

-- Business question 3
CREATE VIEW mart.v_pending_items_by_product AS
SELECT a.product_id, p.product_name, p.barcode, a.pending_quantity, a.pending_lines, a.updated_at
FROM mart.pending_items_by_product a
LEFT JOIN core.dim_product p ON p.product_id = a.product_id AND p.is_current;

-- Business question 4
CREATE VIEW mart.v_top3_customers AS
SELECT customer_id, customer_name, open_orders, orders_status_pending, rnk
FROM (
    SELECT a.customer_id, c.customer_name, a.open_orders, a.orders_status_pending,
           RANK() OVER (ORDER BY a.open_orders DESC) AS rnk
    FROM mart.pending_orders_by_customer a
    LEFT JOIN core.dim_customer c ON c.customer_id = a.customer_id AND c.is_current
) t
WHERE rnk <= 3;

-- Historical question: what was open at any point in time (replayed from the event fact)
CREATE FUNCTION mart.open_orders_as_of(p_ts TIMESTAMP)
RETURNS TABLE (delivery_date DATE, status TEXT, open_orders BIGINT)
LANGUAGE sql STABLE AS $$
    SELECT e.delivery_date, e.status, COUNT(*)
    FROM (
        SELECT DISTINCT ON (order_id) order_id, delivery_date, status, event_type
        FROM core.fact_order_status_events
        WHERE event_ts <= p_ts
        ORDER BY order_id, event_ts DESC, source_lsn DESC, kafka_offset DESC
    ) e
    JOIN core.dim_order_status s USING (status)
    WHERE s.is_open AND e.event_type <> 'delete'
    GROUP BY 1, 2
$$;

-- -----------------------------------------------------------------------------
-- ops: observability
-- -----------------------------------------------------------------------------
CREATE TABLE ops.batch_log (
    batch_id           BIGINT PRIMARY KEY,
    started_at         TIMESTAMPTZ NOT NULL,
    committed_at       TIMESTAMPTZ NOT NULL,
    input_rows         BIGINT NOT NULL,
    customers_rows     BIGINT NOT NULL,
    products_rows      BIGINT NOT NULL,
    orders_rows        BIGINT NOT NULL,
    order_items_rows   BIGINT NOT NULL,
    dead_letter_rows   BIGINT NOT NULL,
    min_source_ts      TIMESTAMP,
    max_source_ts      TIMESTAMP,
    stage_ms           BIGINT NOT NULL,      -- Spark parse + JDBC write
    merge_ms           BIGINT NOT NULL,      -- Postgres transactional merge
    e2e_latency_ms     BIGINT                -- committed_at - max_source_ts
);

CREATE TABLE ops.streaming_progress (
    id                         BIGSERIAL PRIMARY KEY,
    query_name                 TEXT,
    run_id                     TEXT,
    batch_id                   BIGINT,
    progress_ts                TIMESTAMPTZ,
    num_input_rows             BIGINT,
    input_rows_per_second      DOUBLE PRECISION,
    processed_rows_per_second  DOUBLE PRECISION,
    trigger_execution_ms       BIGINT,
    add_batch_ms               BIGINT,
    get_batch_ms               BIGINT,
    latest_offset_ms           BIGINT,
    wal_commit_ms              BIGINT,
    sources_json               JSONB
);

CREATE TABLE ops.dead_letter (
    id               BIGSERIAL PRIMARY KEY,
    batch_id         BIGINT,
    topic            TEXT,
    kafka_partition  INT,
    kafka_offset     BIGINT,
    reason           TEXT,
    raw_value        TEXT,
    captured_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE VIEW ops.v_freshness AS
SELECT MAX(committed_at)                                       AS last_commit_at,
       MAX(max_source_ts)                                      AS last_source_event_ts,
       EXTRACT(EPOCH FROM (now() - MAX(committed_at)))::INT    AS seconds_since_last_commit,
       (SELECT e2e_latency_ms FROM ops.batch_log
         WHERE input_rows > 0 ORDER BY batch_id DESC LIMIT 1)  AS last_e2e_latency_ms
FROM ops.batch_log;
