"""Unit tests for the Debezium parsing. Run inside the image:
    docker compose run --rm --no-deps spark-app python -m pytest -q tests
"""
import datetime as dt
import json

import pytest
from pyspark.sql import SparkSession

from transform import dead_letters, parse_envelope, to_stage

T = "finance_db.operations."


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[1]")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()


def _raw(spark, rows):
    return spark.createDataFrame(rows, "topic string, kafka_partition int, kafka_offset long, value string")


def _env(op, before, after, lsn=100, ts=1793838615156):
    return json.dumps({"before": before, "after": after, "op": op, "ts_ms": ts,
                       "source": {"ts_ms": ts, "lsn": lsn, "snapshot": "false", "table": "x"}})


ORDER = {"order_id": 7, "order_date": 20762, "delivery_date": 20772, "customer_id": 3,
         "status": "PROCESSING", "updated_at": 1793838600895, "updated_by": -1,
         "created_at": 1793838600895, "created_by": -1}


def test_order_update_keeps_before_status_and_types(spark):
    before = dict(ORDER, status="PENDING")
    raw = _raw(spark, [(T + "orders", 0, 10, _env("u", before, ORDER))])
    row = to_stage(parse_envelope(raw, "orders"), "orders").first()
    assert row.order_id == 7
    assert row.order_date == dt.date(2026, 11, 5)
    assert row.delivery_date == dt.date(2026, 11, 15)
    assert row.status == "PROCESSING" and row.before_status == "PENDING"
    assert row.is_deleted is False and row.source_lsn == 100 and row.kafka_offset == 10


def test_delete_uses_before_image(spark):
    raw = _raw(spark, [(T + "orders", 0, 11, _env("d", ORDER, None))])
    row = to_stage(parse_envelope(raw, "orders"), "orders").first()
    assert row.order_id == 7 and row.is_deleted is True and row.op == "d"


def test_misspelled_source_column_and_decimal(spark):
    item = {"order_item_id": 1, "order_id": 7, "product_id": 2, "quanity": 42,
            "updated_at": 1, "updated_by": -1, "created_at": 1, "created_by": -1}
    prod = {"product_id": 2, "product_name": "Car", "barcode": "1", "unity_price": "30.38",
            "is_active": True, "updated_at": 1, "updated_by": -1, "created_at": 1, "created_by": -1}
    raw = _raw(spark, [(T + "order_items", 0, 1, _env("c", None, item)),
                       (T + "products", 0, 1, _env("r", None, prod))])
    assert to_stage(parse_envelope(raw, "order_items"), "order_items").first().quantity == 42
    assert str(to_stage(parse_envelope(raw, "products"), "products").first().unity_price) == "30.3800"


def test_bad_records_go_to_dead_letter(spark):
    raw = _raw(spark, [
        (T + "orders", 0, 1, "not json"),
        (T + "orders", 0, 2, None),
        (T + "orders", 0, 3, _env("u", None, dict(ORDER, order_id=None))),
        (T + "orders", 0, 4, _env("c", None, ORDER)),
    ])
    parsed = parse_envelope(raw, "orders")
    assert to_stage(parsed, "orders").count() == 1
    reasons = sorted(r.reason for r in dead_letters(parsed, "orders", 5).collect())
    assert reasons == ["null primary key", "null value (tombstone)", "unparseable json"]
