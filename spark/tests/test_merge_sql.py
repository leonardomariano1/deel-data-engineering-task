"""Integration tests of the transactional merge (merge_sql.APPLY_BATCH) on a real Postgres.

Each test run creates a throwaway database from analytics-db/init/01_schema.sql, feeds
hand-written CDC rows into the stage tables, applies them exactly like the pipeline does
and checks the marts, the facts, the history and the SCD2 dimension. After every batch the
incrementally maintained marts are compared with a full recompute from the facts.

    docker compose run --rm --no-deps -v ./analytics-db/init:/schema:ro spark-app \
        python3 -m pytest -q tests/test_merge_sql.py
"""
import os
import uuid
from datetime import date, datetime

import psycopg2
import pytest

import merge_sql

SCHEMA = os.getenv("SCHEMA_SQL", "/schema/01_schema.sql")
PG = dict(host=os.getenv("ANALYTICS_DB_HOST", "analytics-db"), port=os.getenv("ANALYTICS_DB_PORT", "5432"),
          user=os.getenv("ANALYTICS_DB_USER", "analytics"), password=os.getenv("ANALYTICS_DB_PASSWORD", "analytics"))

D1, D2, D3 = date(2026, 11, 10), date(2026, 11, 11), date(2026, 11, 12)
TS = datetime(2026, 11, 5, 12, 0, 0)


@pytest.fixture(scope="module")
def db():
    if not os.path.exists(SCHEMA):
        pytest.skip(f"schema file not mounted at {SCHEMA}")
    name = "merge_test_" + uuid.uuid4().hex[:8]
    admin = psycopg2.connect(dbname="postgres", **PG)
    admin.autocommit = True
    admin.cursor().execute(f"CREATE DATABASE {name}")
    conn = psycopg2.connect(dbname=name, **PG)
    with conn, conn.cursor() as cur:
        cur.execute(open(SCHEMA).read())
    yield conn
    conn.close()
    admin.cursor().execute(f"DROP DATABASE {name}")
    admin.close()


# ---------------------------------------------------------------- helpers
def order(order_id, status, dd, customer, lsn, off, op="u", before=None):
    b = before or {}
    return ("orders", dict(order_id=order_id, order_date=D1, delivery_date=dd, customer_id=customer,
                           status=status, before_status=b.get("status"),
                           before_delivery_date=b.get("delivery_date"), before_customer_id=b.get("customer_id"),
                           created_at=TS, updated_at=TS, op=op, is_deleted=op == "d",
                           source_lsn=lsn, source_ts=TS, kafka_partition=0, kafka_offset=off))


def item(item_id, order_id, product, qty, lsn, off, op="c"):
    return ("order_items", dict(order_item_id=item_id, order_id=order_id, product_id=product, quantity=qty,
                                created_at=TS, updated_at=TS, op=op, is_deleted=op == "d",
                                source_lsn=lsn, source_ts=TS, kafka_partition=0, kafka_offset=off))


def customer(cid, name, address, lsn, off, op="u"):
    return ("customers", dict(customer_id=cid, customer_name=name, customer_address=address, is_active=True,
                              created_at=TS, updated_at=TS, op=op, is_deleted=op == "d",
                              source_lsn=lsn, source_ts=TS, kafka_partition=0, kafka_offset=off))


def apply(conn, rows):
    """Same sequence as the pipeline: truncate stage, load the batch, one transaction."""
    with conn, conn.cursor() as cur:
        cur.execute(merge_sql.TRUNCATE_STAGE)
        for table, r in rows:
            cols = ", ".join(r)
            cur.execute(f"INSERT INTO stage.{table}_cdc ({cols}) VALUES ({', '.join(['%s'] * len(r))})",
                        list(r.values()))
    with conn, conn.cursor() as cur:
        cur.execute(merge_sql.APPLY_BATCH)
    assert_marts_equal_full_recompute(conn)


def q(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def marts(conn):
    return (
        dict(((d, s), n) for d, s, n in q(conn, "SELECT delivery_date, status, open_orders FROM mart.open_orders_by_delivery_date_status")),
        dict((p, (a, b)) for p, a, b in q(conn, "SELECT product_id, pending_quantity, pending_lines FROM mart.pending_items_by_product")),
        dict((c, (a, b)) for c, a, b in q(conn, "SELECT customer_id, open_orders, orders_status_pending FROM mart.pending_orders_by_customer")),
    )


def assert_marts_equal_full_recompute(conn):
    full_q1 = dict(((d, s), n) for d, s, n in q(conn, """
        SELECT delivery_date, status, COUNT(*) FROM core.v_order_state WHERE is_open GROUP BY 1, 2"""))
    full_q3 = dict((p, (int(a), b)) for p, a, b in q(conn, """
        SELECT i.product_id, SUM(COALESCE(i.quantity, 0)), COUNT(*) FROM core.fact_order_items i
        JOIN core.v_order_state o ON o.order_id = i.order_id
        WHERE o.is_open AND NOT i.is_deleted GROUP BY 1"""))
    full_q4 = dict((c, (a, b)) for c, a, b in q(conn, """
        SELECT customer_id, COUNT(*), COUNT(*) FILTER (WHERE status = 'PENDING')
        FROM core.v_order_state WHERE is_open GROUP BY 1"""))
    assert marts(conn) == (full_q1, full_q3, full_q4)


# ---------------------------------------------------------------- scenarios (run in order)
BATCH_1 = [
    order(100, "PENDING", D1, 1, lsn=10, off=0, op="r"),
    item(1000, 100, 10, 5, lsn=10, off=0, op="r"),
    item(1001, 100, 10, 3, lsn=10, off=1, op="r"),
    customer(1, "ACME", "Street 1", lsn=10, off=0, op="r"),
]


def test_snapshot(db):
    apply(db, BATCH_1)
    assert marts(db) == ({(D1, "PENDING"): 1}, {10: (8, 2)}, {1: (1, 1)})


def test_replay_of_a_committed_batch_changes_nothing(db):
    before = (marts(db), q(db, "SELECT count(*) FROM core.fact_order_status_events"),
              q(db, "SELECT count(*) FROM core.dim_customer"))
    apply(db, BATCH_1)
    after = (marts(db), q(db, "SELECT count(*) FROM core.fact_order_status_events"),
             q(db, "SELECT count(*) FROM core.dim_customer"))
    assert after == before


def test_item_before_its_order(db):
    apply(db, [item(1002, 101, 10, 7, lsn=20, off=2)])
    assert marts(db)[1] == {10: (8, 2)}          # order 101 unknown: contributes nothing yet
    apply(db, [order(101, "PROCESSING", D2, 1, lsn=21, off=1, op="c")])
    assert marts(db)[1] == {10: (15, 3)}         # the order pulls its early item in
    assert marts(db)[2] == {1: (2, 1)}


def test_completion_and_stale_event_in_same_batch(db):
    apply(db, [
        order(100, "COMPLETED", D1, 1, lsn=30, off=2, before={"status": "PENDING", "delivery_date": D1, "customer_id": 1}),
        order(101, "PENDING", D2, 1, lsn=5, off=0),   # older than what is stored: ignored
    ])
    assert marts(db) == ({(D2, "PROCESSING"): 1}, {10: (7, 1)}, {1: (1, 0)})


def test_delivery_date_change_without_status_change_is_history(db):
    apply(db, [order(101, "PROCESSING", D3, 1, lsn=40, off=3,
                     before={"status": "PROCESSING", "delivery_date": D2, "customer_id": 1})])
    assert marts(db)[0] == {(D3, "PROCESSING"): 1}
    assert q(db, "SELECT delivery_date FROM core.fact_order_status_events WHERE order_id = 101 ORDER BY source_lsn DESC LIMIT 1") == [(D3,)]
    assert q(db, "SELECT * FROM mart.open_orders_as_of('2026-11-05 12:00:00') ORDER BY 1, 2") == [(D3, "PROCESSING", 1)]


def test_delete_order(db):
    apply(db, [order(101, "PROCESSING", D3, 1, lsn=50, off=4, op="d")])
    assert marts(db) == ({}, {}, {})
    assert q(db, "SELECT event_type FROM core.fact_order_status_events WHERE order_id = 101 ORDER BY source_lsn DESC LIMIT 1") == [("delete",)]


def test_scd2_only_versions_real_changes(db):
    apply(db, [
        customer(1, "ACME", "Street 1", lsn=60, off=1),   # no change: no new version
        customer(1, "ACME", "Street 2", lsn=61, off=2),   # address change: new version
        customer(1, "ACME", "Street 2", lsn=62, off=3),   # no change again
    ])
    rows = q(db, "SELECT customer_address, is_current, valid_to IS NULL FROM core.dim_customer WHERE customer_id = 1 ORDER BY source_lsn")
    assert rows == [("Street 1", False, False), ("Street 2", True, True)]
    apply(db, [customer(1, "ACME", "Street 2", lsn=61, off=2)])  # replay
    assert q(db, "SELECT count(*) FROM core.dim_customer WHERE customer_id = 1") == [(2,)]
