"""Debezium envelope schemas and the per-table column mapping.

The connector uses JsonConverter with schemas disabled, decimal.handling.mode=string
and time.precision.mode=connect, so on the wire:
  DATE       -> int, days since epoch
  TIMESTAMP  -> long, milliseconds since epoch (source wall clock, UTC)
  NUMERIC    -> string
"""
from dataclasses import dataclass

from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

TOPIC_PREFIX = "finance_db.operations."


def _audit_fields():
    return [
        StructField("updated_at", LongType()),
        StructField("updated_by", LongType()),
        StructField("created_at", LongType()),
        StructField("created_by", LongType()),
    ]


ROW_SCHEMAS = {
    "customers": StructType(
        [
            StructField("customer_id", LongType()),
            StructField("customer_name", StringType()),
            StructField("is_active", BooleanType()),
            StructField("customer_address", StringType()),
            *_audit_fields(),
        ]
    ),
    "products": StructType(
        [
            StructField("product_id", LongType()),
            StructField("product_name", StringType()),
            StructField("barcode", StringType()),
            StructField("unity_price", StringType()),
            StructField("is_active", BooleanType()),
            *_audit_fields(),
        ]
    ),
    "orders": StructType(
        [
            StructField("order_id", LongType()),
            StructField("order_date", IntegerType()),
            StructField("delivery_date", IntegerType()),
            StructField("customer_id", LongType()),
            StructField("status", StringType()),
            *_audit_fields(),
        ]
    ),
    "order_items": StructType(
        [
            StructField("order_item_id", LongType()),
            StructField("order_id", LongType()),
            StructField("product_id", LongType()),
            # the source column is misspelled ("quanity"); fixed in the analytical model
            StructField("quanity", IntegerType()),
            *_audit_fields(),
        ]
    ),
}

SOURCE_SCHEMA = StructType(
    [
        StructField("ts_ms", LongType()),
        StructField("snapshot", StringType()),
        StructField("lsn", LongType()),
        StructField("txId", LongType()),
        StructField("table", StringType()),
    ]
)


def envelope_schema(table: str) -> StructType:
    row = ROW_SCHEMAS[table]
    return StructType(
        [
            StructField("before", row),
            StructField("after", row),
            StructField("source", SOURCE_SCHEMA),
            StructField("op", StringType()),
            StructField("ts_ms", LongType()),
        ]
    )


@dataclass(frozen=True)
class TableSpec:
    name: str
    key: str
    stage_table: str


TABLES = [
    TableSpec("customers", "customer_id", "stage.customers_cdc"),
    TableSpec("products", "product_id", "stage.products_cdc"),
    TableSpec("orders", "order_id", "stage.orders_cdc"),
    TableSpec("order_items", "order_item_id", "stage.order_items_cdc"),
]
