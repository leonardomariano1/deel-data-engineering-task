"""Pure DataFrame transformations: raw Kafka records -> typed CDC rows per table.

Kept free of I/O so it can be unit tested with a local SparkSession.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from cdc_schemas import TOPIC_PREFIX, envelope_schema

VALID_OPS = ["c", "u", "d", "r"]


def _ms_to_ts(c):
    return F.timestamp_millis(c)


def _days_to_date(c):
    return F.date_from_unix_date(c)


def parse_envelope(raw: DataFrame, table: str) -> DataFrame:
    """Parse the Debezium JSON for one table.

    Output columns: kafka metadata, op, the row image (after, or before for deletes),
    before_* fields that downstream needs, and a `dq_error` column (null when valid).
    """
    env = F.from_json(F.col("value"), envelope_schema(table))
    df = (
        raw.where(F.col("topic") == F.lit(TOPIC_PREFIX + table))
        .withColumn("env", env)
        .withColumn("row", F.coalesce(F.col("env.after"), F.col("env.before")))
    )
    key_col = {
        "customers": "customer_id",
        "products": "product_id",
        "orders": "order_id",
        "order_items": "order_item_id",
    }[table]
    dq_error = (
        F.when(F.col("value").isNull(), F.lit("null value (tombstone)"))
        # PERMISSIVE from_json yields null or an all-null struct for malformed input
        .when(F.col("env").isNull() | F.col("env.op").isNull(), F.lit("unparseable json"))
        .when(~F.col("env.op").isin(VALID_OPS), F.lit("unknown op"))
        .when(F.col("row").isNull(), F.lit("no row image"))
        .when(F.col(f"row.{key_col}").isNull(), F.lit("null primary key"))
        .when(F.col("env.source.lsn").isNull(), F.lit("missing source lsn"))
    )
    return df.withColumn("dq_error", dq_error)


def _common(df: DataFrame):
    return [
        F.col("env.op").alias("op"),
        (F.col("env.op") == "d").alias("is_deleted"),
        F.col("env.source.lsn").alias("source_lsn"),
        _ms_to_ts(F.col("env.source.ts_ms")).alias("source_ts"),
        F.col("kafka_partition").cast("int").alias("kafka_partition"),
        F.col("kafka_offset"),
    ]


def to_stage(parsed: DataFrame, table: str) -> DataFrame:
    """Select the stage-table columns from a parsed (valid) envelope DataFrame."""
    ok = parsed.where(F.col("dq_error").isNull())
    audit = [
        _ms_to_ts(F.col("row.created_at")).alias("created_at"),
        _ms_to_ts(F.col("row.updated_at")).alias("updated_at"),
    ]
    if table == "customers":
        cols = [
            F.col("row.customer_id").alias("customer_id"),
            F.col("row.customer_name").alias("customer_name"),
            F.col("row.customer_address").alias("customer_address"),
            F.col("row.is_active").alias("is_active"),
        ]
    elif table == "products":
        cols = [
            F.col("row.product_id").alias("product_id"),
            F.col("row.product_name").alias("product_name"),
            F.col("row.barcode").alias("barcode"),
            F.col("row.unity_price").cast("decimal(18,4)").alias("unity_price"),
            F.col("row.is_active").alias("is_active"),
        ]
    elif table == "orders":
        cols = [
            F.col("row.order_id").alias("order_id"),
            _days_to_date(F.col("row.order_date")).alias("order_date"),
            _days_to_date(F.col("row.delivery_date")).alias("delivery_date"),
            F.col("row.customer_id").alias("customer_id"),
            F.col("row.status").alias("status"),
            F.col("env.before.status").alias("before_status"),
            _days_to_date(F.col("env.before.delivery_date")).alias("before_delivery_date"),
            F.col("env.before.customer_id").alias("before_customer_id"),
        ]
    elif table == "order_items":
        cols = [
            F.col("row.order_item_id").alias("order_item_id"),
            F.col("row.order_id").alias("order_id"),
            F.col("row.product_id").alias("product_id"),
            F.col("row.quanity").alias("quantity"),
        ]
    else:
        raise ValueError(table)
    return ok.select(*cols, *audit, *_common(ok))


def dead_letters(parsed: DataFrame, table: str, batch_id: int) -> DataFrame:
    return parsed.where(F.col("dq_error").isNotNull()).select(
        F.lit(batch_id).cast("long").alias("batch_id"),
        F.col("topic"),
        F.col("kafka_partition").cast("int").alias("kafka_partition"),
        F.col("kafka_offset"),
        F.col("dq_error").alias("reason"),
        F.col("value").alias("raw_value"),
    )
