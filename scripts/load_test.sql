-- Load test: pushes ~1.6M CDC events through Debezium -> Kafka -> Spark.
-- Run against the SOURCE database:
--   docker compose exec -T transactions-db psql -U finance_db_user -d finance_db -f - < scripts/load_test.sql
-- Orders get delivery dates skewed towards the next days (random()^2 over 30 days) and 4 items each, then a
-- third of them change status, so every mart and the history table are exercised.
\if :{?orders} \else \set orders 300000 \endif
\timing on

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
WHERE o.created_by = -2
  AND NOT EXISTS (SELECT 1 FROM operations.order_items x WHERE x.order_id = o.order_id);

UPDATE operations.orders
SET status = (ARRAY['PROCESSING', 'REPROCESSING', 'COMPLETED'])[1 + floor(random() * 3)::int],
    updated_at = clock_timestamp(), updated_by = -2
WHERE created_by = -2 AND status = 'PENDING' AND random() < 0.4;
