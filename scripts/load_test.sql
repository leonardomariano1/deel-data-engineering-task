-- Load test: pushes ~1.6M CDC events through Debezium -> Kafka -> Spark.
-- Run against the SOURCE database (optional, the provided generator alone keeps
-- only a handful of orders open):
--   docker compose exec -T transactions-db psql -U finance_db_user -d finance_db -f - < scripts/load_test.sql
-- Orders get delivery dates skewed towards the next days (random()^2 over 30 days) and 4 items
-- each; then about 40% change status and 1% are deleted,
-- so every mart, the history table and the delete path are exercised.
\if :{?orders} \else \set orders 300000 \endif
\timing on

-- only the rows created by this run are touched below
SELECT COALESCE(MAX(order_id), 0) AS start_id FROM operations.orders \gset

INSERT INTO operations.orders (order_date, delivery_date, customer_id, status, updated_at, updated_by, created_at, created_by)
SELECT CURRENT_DATE,
       CURRENT_DATE + floor(30 * random() ^ 2)::int,
       c.ids[1 + floor(array_length(c.ids, 1) * random() ^ 1.5)::int],
       'PENDING', clock_timestamp(), -2, clock_timestamp(), -2
FROM generate_series(1, :orders) g,
     (SELECT array_agg(customer_id) ids FROM operations.customers) c;

INSERT INTO operations.order_items (order_id, product_id, quanity, updated_at, updated_by, created_at, created_by)
SELECT o.order_id,
       p.ids[1 + ((o.order_id * 7 + n) % array_length(p.ids, 1))],
       1 + ((o.order_id * 31 + n * 17) % 50),
       clock_timestamp(), -2, clock_timestamp(), -2
FROM operations.orders o
CROSS JOIN generate_series(1, 4) n
CROSS JOIN (SELECT array_agg(product_id) ids FROM operations.products) p
WHERE o.order_id > :start_id;

UPDATE operations.orders
SET status = (ARRAY['PROCESSING', 'REPROCESSING', 'COMPLETED'])[1 + floor(random() * 3)::int],
    updated_at = clock_timestamp(), updated_by = -2
WHERE order_id > :start_id AND status = 'PENDING' AND random() < 0.4;

-- Deletes are never produced by the generator: remove a small sample of the new orders
-- (and their items) so the delete path of the pipeline is exercised too.
WITH victims AS (
    SELECT order_id FROM operations.orders
    WHERE order_id > :start_id AND random() < 0.01
)
, del_items AS (
    DELETE FROM operations.order_items i USING victims v WHERE i.order_id = v.order_id
)
DELETE FROM operations.orders o USING victims v WHERE o.order_id = v.order_id;
