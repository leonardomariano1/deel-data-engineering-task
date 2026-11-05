"""Every query the serving layer runs. The API, the CLI and the reconciliation
share this module so the numbers are always produced by the same SQL."""

OPEN_ORDERS = """
SELECT delivery_date, day_name, status, open_orders, updated_at
FROM mart.v_open_orders_by_delivery_date_status
WHERE (%(delivery_date)s::date IS NULL OR delivery_date = %(delivery_date)s::date)
  AND (%(status)s::text IS NULL OR status = %(status)s::text)
ORDER BY delivery_date, status
"""

TOP_DELIVERY_DATES = "SELECT delivery_date, open_orders, rnk FROM mart.v_top3_delivery_dates ORDER BY rnk, delivery_date"

PENDING_ITEMS = """
SELECT product_id, product_name, pending_quantity, pending_lines, updated_at
FROM mart.v_pending_items_by_product ORDER BY pending_quantity DESC, product_id
"""

TOP_CUSTOMERS = """
SELECT customer_id, customer_name, open_orders, orders_status_pending, rnk
FROM mart.v_top3_customers ORDER BY rnk, customer_id
"""

TOP_CUSTOMERS_PENDING_STATUS = """
SELECT customer_id, customer_name, orders_status_pending, open_orders, rnk
FROM mart.v_top3_customers_pending_status ORDER BY rnk, customer_id
"""

OPEN_ORDERS_AS_OF = "SELECT * FROM mart.open_orders_as_of(%(as_of)s::timestamp) ORDER BY 1, 2"

ORDER_TIMELINE = """
SELECT event_type, previous_status, status, event_ts, source_lsn
FROM core.fact_order_status_events WHERE order_id = %(order_id)s
ORDER BY event_ts, source_lsn, kafka_offset
"""

FRESHNESS = "SELECT * FROM ops.v_freshness"

RECENT_BATCHES = """
SELECT batch_id, committed_at, input_rows, orders_rows, order_items_rows,
       customers_rows, products_rows, dead_letter_rows, stage_ms, merge_ms, e2e_latency_ms
FROM ops.batch_log ORDER BY batch_id DESC LIMIT %(limit)s
"""

RECENT_PROGRESS = """
SELECT batch_id, progress_ts, num_input_rows, input_rows_per_second,
       processed_rows_per_second, trigger_execution_ms, add_batch_ms, latest_offset_ms
FROM ops.streaming_progress ORDER BY id DESC LIMIT %(limit)s
"""

TOTALS = """
SELECT (SELECT COALESCE(SUM(open_orders), 0) FROM mart.open_orders_by_delivery_date_status) AS open_orders,
       (SELECT COALESCE(SUM(pending_quantity), 0) FROM mart.pending_items_by_product)       AS pending_quantity,
       (SELECT COUNT(*) FROM core.fact_orders)                                              AS orders_total,
       (SELECT COUNT(*) FROM core.fact_order_items)                                         AS items_total,
       (SELECT COUNT(*) FROM core.fact_order_status_events)                                 AS status_events
"""

# --- reconciliation: same metrics computed straight from the OLTP source ---------------
SOURCE = {
    "open_orders_by_delivery_date_status": """
        SELECT delivery_date::text, status, COUNT(*) FROM operations.orders
        WHERE status <> 'COMPLETED' GROUP BY 1, 2""",
    "pending_items_by_product": """
        SELECT i.product_id::text, SUM(i.quanity), COUNT(*) FROM operations.order_items i
        JOIN operations.orders o USING (order_id) WHERE o.status <> 'COMPLETED' GROUP BY 1""",
    "pending_orders_by_customer": """
        SELECT customer_id::text, COUNT(*), COUNT(*) FILTER (WHERE status = 'PENDING')
        FROM operations.orders WHERE status <> 'COMPLETED' GROUP BY 1""",
}
MART = {
    "open_orders_by_delivery_date_status": """
        SELECT delivery_date::text, status, open_orders FROM mart.open_orders_by_delivery_date_status""",
    "pending_items_by_product": """
        SELECT product_id::text, pending_quantity, pending_lines FROM mart.pending_items_by_product""",
    "pending_orders_by_customer": """
        SELECT customer_id::text, open_orders, orders_status_pending FROM mart.pending_orders_by_customer""",
}
