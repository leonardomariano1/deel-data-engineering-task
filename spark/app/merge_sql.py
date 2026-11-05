"""SQL applied by the driver, in ONE Postgres transaction per micro-batch.

Why a single transaction:
  * dimensions, facts, history and the serving aggregates move together, so a
    reader never sees an aggregate that disagrees with the facts behind it;
  * every statement is idempotent (version guards on facts, ON CONFLICT DO NOTHING
    on history and dimensions, deltas computed from the actual before/after state),
    so a micro-batch that Spark replays after a failure produces no double counting.

Aggregates are maintained with deltas (new contribution minus old contribution of the
rows touched by the batch) instead of full recomputes, so the cost of a batch is
proportional to the batch size, not to the size of the history.
"""
from __future__ import annotations


# 1. Affected keys and the state BEFORE the batch is applied --------------------------
CAPTURE_OLD_STATE = """
CREATE TEMP TABLE t_orders ON COMMIT DROP AS
    SELECT DISTINCT order_id FROM stage.orders_cdc;

-- items whose contribution can change: items in the batch + items of orders in the batch
CREATE TEMP TABLE t_items ON COMMIT DROP AS
    SELECT order_item_id FROM stage.order_items_cdc
    UNION
    SELECT i.order_item_id FROM core.fact_order_items i JOIN t_orders USING (order_id);

CREATE TEMP TABLE t_old_orders ON COMMIT DROP AS
    SELECT v.order_id, v.customer_id, v.delivery_date, v.status, v.is_open
    FROM core.v_order_state v JOIN t_orders USING (order_id);

CREATE TEMP TABLE t_old_items ON COMMIT DROP AS
    SELECT i.order_item_id, i.product_id, COALESCE(i.quantity, 0) AS quantity
    FROM core.fact_order_items i
    JOIN t_items USING (order_item_id)
    JOIN core.v_order_state v ON v.order_id = i.order_id
    WHERE NOT i.is_deleted AND v.is_open;
"""

# 2. SCD2 dimensions ---------------------------------------------------------------------
def scd2_sql(dim: str, stage: str, key: str, attrs: list[str]) -> str:
    cols = ", ".join(attrs)
    fingerprint = "md5(concat_ws('|', " + ", ".join(f"{a}::text" for a in attrs) + ", is_deleted::text))"
    return f"""
-- New version only when a tracked attribute (or the deleted flag) actually changed, and only
-- for events newer than the latest version already stored (replays and stale events skip).
INSERT INTO {dim} ({key}, {cols}, valid_from, is_deleted, source_lsn, kafka_offset)
SELECT {key}, {cols}, valid_from, is_deleted, source_lsn, kafka_offset
FROM (
    SELECT u.*,
           LAG(fp) OVER (PARTITION BY {key} ORDER BY pos) AS prev_fp,
           MAX(CASE WHEN NOT is_new THEN pos END) OVER (PARTITION BY {key}) AS stored_pos
    FROM (
        SELECT {key}, {cols}, is_deleted, source_lsn, kafka_offset,
               CASE WHEN is_deleted THEN source_ts ELSE COALESCE(updated_at, source_ts) END AS valid_from,
               {fingerprint} AS fp,
               source_lsn::numeric * 1e12 + kafka_offset AS pos,
               TRUE AS is_new
        FROM {stage}
        UNION ALL
        SELECT {key}, {cols}, is_deleted, source_lsn, kafka_offset, valid_from,
               {fingerprint}, source_lsn::numeric * 1e12 + kafka_offset, FALSE
        FROM {dim}
        WHERE valid_to IS NULL AND {key} IN (SELECT {key} FROM {stage})
    ) u
) x
WHERE is_new
  AND (stored_pos IS NULL OR pos > stored_pos)
  AND prev_fp IS DISTINCT FROM fp
ON CONFLICT ({key}, source_lsn, kafka_offset) DO NOTHING;

-- re-derive validity only for the open versions of the keys touched by this batch
CREATE TEMP TABLE t_scd ON COMMIT DROP AS
SELECT d.{key.replace('_id', '_sk')} AS sk, d.is_deleted,
       LEAD(d.valid_from) OVER w AS next_from,
       ROW_NUMBER() OVER (PARTITION BY d.{key} ORDER BY d.source_lsn DESC, d.kafka_offset DESC) AS rn
FROM {dim} d
WHERE d.{key} IN (SELECT {key} FROM {stage})
  AND (d.valid_to IS NULL OR d.is_current)
WINDOW w AS (PARTITION BY d.{key} ORDER BY d.source_lsn, d.kafka_offset);

UPDATE {dim} d SET valid_to = t.next_from, is_current = FALSE
FROM t_scd t WHERE d.{key.replace('_id', '_sk')} = t.sk AND t.rn > 1;

UPDATE {dim} d SET valid_to = NULL, is_current = NOT t.is_deleted
FROM t_scd t WHERE d.{key.replace('_id', '_sk')} = t.sk AND t.rn = 1;

DROP TABLE t_scd;
"""


MERGE_DIMENSIONS = scd2_sql(
    "core.dim_customer", "stage.customers_cdc", "customer_id",
    ["customer_name", "customer_address", "is_active"],
) + scd2_sql(
    "core.dim_product", "stage.products_cdc", "product_id",
    ["product_name", "barcode", "unity_price", "is_active"],
)

# 3. Facts: latest version wins, guarded by (lsn, offset) so replays and late events are no-ops
MERGE_FACTS = """
INSERT INTO core.fact_orders AS f
    (order_id, customer_id, order_date, delivery_date, status,
     created_at, updated_at, is_deleted, source_lsn, kafka_offset)
SELECT DISTINCT ON (order_id)
       order_id, customer_id, order_date, delivery_date, status,
       created_at, updated_at, is_deleted, source_lsn, kafka_offset
FROM stage.orders_cdc
ORDER BY order_id, source_lsn DESC, kafka_offset DESC
ON CONFLICT (order_id) DO UPDATE SET
    customer_id   = EXCLUDED.customer_id,
    order_date    = EXCLUDED.order_date,
    delivery_date = EXCLUDED.delivery_date,
    status        = EXCLUDED.status,
    created_at    = EXCLUDED.created_at,
    updated_at    = EXCLUDED.updated_at,
    is_deleted    = EXCLUDED.is_deleted,
    source_lsn    = EXCLUDED.source_lsn,
    kafka_offset  = EXCLUDED.kafka_offset,
    loaded_at     = now()
WHERE (EXCLUDED.source_lsn, EXCLUDED.kafka_offset) > (f.source_lsn, f.kafka_offset);

INSERT INTO core.fact_order_items AS f
    (order_item_id, order_id, product_id, quantity,
     created_at, updated_at, is_deleted, source_lsn, kafka_offset)
SELECT DISTINCT ON (order_item_id)
       order_item_id, order_id, product_id, quantity,
       created_at, updated_at, is_deleted, source_lsn, kafka_offset
FROM stage.order_items_cdc
ORDER BY order_item_id, source_lsn DESC, kafka_offset DESC
ON CONFLICT (order_item_id) DO UPDATE SET
    order_id     = EXCLUDED.order_id,
    product_id   = EXCLUDED.product_id,
    quantity     = EXCLUDED.quantity,
    created_at   = EXCLUDED.created_at,
    updated_at   = EXCLUDED.updated_at,
    is_deleted   = EXCLUDED.is_deleted,
    source_lsn   = EXCLUDED.source_lsn,
    kafka_offset = EXCLUDED.kafka_offset,
    loaded_at    = now()
WHERE (EXCLUDED.source_lsn, EXCLUDED.kafka_offset) > (f.source_lsn, f.kafka_offset);

-- history: creations, deletions and every change of status, delivery date or customer,
-- exactly once per source change (order_id, LSN)
INSERT INTO core.fact_order_status_events
    (order_id, customer_id, delivery_date, previous_status, status, event_type,
     event_ts, source_lsn, kafka_partition, kafka_offset)
SELECT order_id, customer_id, delivery_date, before_status, status,
       CASE op WHEN 'r' THEN 'snapshot' WHEN 'c' THEN 'insert'
               WHEN 'd' THEN 'delete' ELSE 'update' END,
       CASE WHEN op = 'd' THEN source_ts ELSE COALESCE(updated_at, source_ts) END,
       source_lsn, kafka_partition, kafka_offset
FROM stage.orders_cdc
WHERE op IN ('r', 'c', 'd')
   OR before_status        IS DISTINCT FROM status
   OR before_delivery_date IS DISTINCT FROM delivery_date
   OR before_customer_id   IS DISTINCT FROM customer_id
ON CONFLICT (order_id, source_lsn) DO NOTHING;
"""

# 4. State AFTER the batch, then apply deltas to the serving aggregates -------------------
APPLY_DELTAS = """
CREATE TEMP TABLE t_new_orders ON COMMIT DROP AS
    SELECT v.order_id, v.customer_id, v.delivery_date, v.status, v.is_open
    FROM core.v_order_state v JOIN t_orders USING (order_id);

CREATE TEMP TABLE t_new_items ON COMMIT DROP AS
    SELECT i.order_item_id, i.product_id, COALESCE(i.quantity, 0) AS quantity
    FROM core.fact_order_items i
    JOIN t_items USING (order_item_id)
    JOIN core.v_order_state v ON v.order_id = i.order_id
    WHERE NOT i.is_deleted AND v.is_open;

-- Q1/Q2: open orders by delivery date and status
INSERT INTO mart.open_orders_by_delivery_date_status AS m (delivery_date, status, open_orders, updated_at)
SELECT delivery_date, status, SUM(d), now()
FROM (
    SELECT delivery_date, COALESCE(status, 'UNKNOWN') AS status,  1 AS d FROM t_new_orders WHERE is_open
    UNION ALL
    SELECT delivery_date, COALESCE(status, 'UNKNOWN') AS status, -1 AS d FROM t_old_orders WHERE is_open
) x
WHERE delivery_date IS NOT NULL
GROUP BY delivery_date, status
HAVING SUM(d) <> 0
ON CONFLICT (delivery_date, status) DO UPDATE SET
    open_orders = m.open_orders + EXCLUDED.open_orders,
    updated_at  = EXCLUDED.updated_at;

DELETE FROM mart.open_orders_by_delivery_date_status WHERE open_orders = 0;

-- Q3: pending items by product
INSERT INTO mart.pending_items_by_product AS m (product_id, pending_quantity, pending_lines, updated_at)
SELECT product_id, SUM(q), SUM(l), now()
FROM (
    SELECT product_id,  quantity AS q,  1 AS l FROM t_new_items
    UNION ALL
    SELECT product_id, -quantity AS q, -1 AS l FROM t_old_items
) x
WHERE product_id IS NOT NULL
GROUP BY product_id
HAVING SUM(q) <> 0 OR SUM(l) <> 0
ON CONFLICT (product_id) DO UPDATE SET
    pending_quantity = m.pending_quantity + EXCLUDED.pending_quantity,
    pending_lines    = m.pending_lines + EXCLUDED.pending_lines,
    updated_at       = EXCLUDED.updated_at;

DELETE FROM mart.pending_items_by_product WHERE pending_lines = 0;

-- Q4: pending orders by customer
INSERT INTO mart.pending_orders_by_customer AS m (customer_id, open_orders, orders_status_pending, updated_at)
SELECT customer_id, SUM(o), SUM(p), now()
FROM (
    SELECT customer_id,  1 AS o, CASE WHEN status = 'PENDING' THEN  1 ELSE 0 END AS p
    FROM t_new_orders WHERE is_open
    UNION ALL
    SELECT customer_id, -1 AS o, CASE WHEN status = 'PENDING' THEN -1 ELSE 0 END AS p
    FROM t_old_orders WHERE is_open
) x
WHERE customer_id IS NOT NULL
GROUP BY customer_id
HAVING SUM(o) <> 0 OR SUM(p) <> 0
ON CONFLICT (customer_id) DO UPDATE SET
    open_orders           = m.open_orders + EXCLUDED.open_orders,
    orders_status_pending = m.orders_status_pending + EXCLUDED.orders_status_pending,
    updated_at            = EXCLUDED.updated_at;

DELETE FROM mart.pending_orders_by_customer WHERE open_orders = 0;
"""

BATCH_LOG = """
INSERT INTO ops.batch_log (
    batch_id, started_at, committed_at, input_rows, customers_rows, products_rows,
    orders_rows, order_items_rows, dead_letter_rows, min_source_ts, max_source_ts,
    stage_ms, merge_ms, e2e_latency_ms, e2e_latency_max_ms)
VALUES (%(batch_id)s, %(started_at)s, clock_timestamp(), %(input_rows)s, %(customers)s,
        %(products)s, %(orders)s, %(order_items)s, %(dead_letters)s, %(min_ts)s, %(max_ts)s,
        %(stage_ms)s, %(merge_ms)s,
        CASE WHEN %(max_ts)s::timestamp IS NULL THEN NULL
             ELSE (EXTRACT(EPOCH FROM clock_timestamp() - (%(max_ts)s::timestamp AT TIME ZONE 'UTC')) * 1000)::BIGINT END,
        CASE WHEN %(min_ts)s::timestamp IS NULL THEN NULL
             ELSE (EXTRACT(EPOCH FROM clock_timestamp() - (%(min_ts)s::timestamp AT TIME ZONE 'UTC')) * 1000)::BIGINT END)
ON CONFLICT (batch_id) DO UPDATE SET
    committed_at = EXCLUDED.committed_at,
    merge_ms     = EXCLUDED.merge_ms;
"""

TRUNCATE_STAGE = """
TRUNCATE stage.customers_cdc, stage.products_cdc, stage.orders_cdc, stage.order_items_cdc;
"""

APPLY_BATCH = CAPTURE_OLD_STATE + MERGE_DIMENSIONS + MERGE_FACTS + APPLY_DELTAS
