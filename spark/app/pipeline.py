"""ACME orders CDC -> dimensional model, with Spark Structured Streaming.

One streaming query reads the four Debezium topics. Each micro-batch:
  1. parses and validates the Debezium envelopes (Spark),
  2. quarantines malformed records to ops.dead_letter (Spark -> JDBC),
  3. writes typed CDC rows to the stage schema (Spark -> JDBC, parallel per partition),
  4. applies dimensions, facts, history and aggregate deltas in ONE Postgres
     transaction (driver, psycopg2).

Kafka offsets are committed to the checkpoint only after foreachBatch returns, and
step 4 is idempotent, so the end-to-end guarantee is effectively exactly-once.
"""
import logging
import os
import time
from datetime import datetime, timezone

import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import merge_sql
from cdc_schemas import TABLES, TOPIC_PREFIX
from progress_listener import ProgressListener
from transform import dead_letters, parse_envelope, to_stage

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
log = logging.getLogger("orders-cdc")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC_PATTERN = os.getenv("TOPIC_PATTERN", r"finance_db\.operations\..*")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "earliest")
MAX_OFFSETS_PER_TRIGGER = int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "200000"))
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "5 seconds")
CHECKPOINT = os.getenv("CHECKPOINT_LOCATION", "/checkpoints/orders_cdc")

PG_HOST = os.getenv("ANALYTICS_DB_HOST", "analytics-db")
PG_PORT = os.getenv("ANALYTICS_DB_PORT", "5432")
PG_DB = os.getenv("ANALYTICS_DB_NAME", "analytics")
PG_USER = os.getenv("ANALYTICS_DB_USER", "analytics")
PG_PASSWORD = os.getenv("ANALYTICS_DB_PASSWORD", "analytics")
JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}?reWriteBatchedInserts=true"
JDBC_PROPS = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver",
    "batchsize": "5000",
}


def pg_connect():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD,
        application_name="orders-cdc-driver",
    )


def jdbc_append(df, table):
    df.write.jdbc(JDBC_URL, table, mode="append", properties=JDBC_PROPS)


def process_batch(batch_df, batch_id: int):
    started = datetime.now(timezone.utc)
    t0 = time.monotonic()

    batch_df.persist()
    counts = {r["topic"]: r["n"] for r in batch_df.groupBy("topic").agg(F.count("*").alias("n")).collect()}
    input_rows = sum(counts.values())
    if input_rows == 0:
        batch_df.unpersist()
        return

    # Stage tables are scratch space for this batch; a replayed batch starts clean.
    conn = pg_connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(merge_sql.TRUNCATE_STAGE)
    finally:
        conn.close()

    per_table = {}
    dead_total = 0
    source_ts = []
    for spec in TABLES:
        n_topic = counts.get(TOPIC_PREFIX + spec.name, 0)
        per_table[spec.name] = 0
        if n_topic == 0:
            continue
        parsed = parse_envelope(batch_df, spec.name).persist()
        dlq = dead_letters(parsed, spec.name, batch_id)
        n_dead = dlq.count()
        if n_dead:
            jdbc_append(dlq, "ops.dead_letter")
            log.warning("batch %s: %s dead letters on %s", batch_id, n_dead, spec.name)
        staged = to_stage(parsed, spec.name)
        jdbc_append(staged, spec.stage_table)
        stats = staged.agg(F.count("*").alias("n"), F.min("source_ts").alias("mn"),
                           F.max("source_ts").alias("mx")).first()
        per_table[spec.name] = stats["n"]
        if stats["mn"] is not None:
            source_ts += [stats["mn"], stats["mx"]]
        dead_total += n_dead
        parsed.unpersist()

    stage_ms = int((time.monotonic() - t0) * 1000)
    t1 = time.monotonic()

    conn = pg_connect()
    try:
        with conn:  # one transaction: commit on success, rollback on any error
            with conn.cursor() as cur:
                cur.execute(merge_sql.APPLY_BATCH)
                merge_ms = int((time.monotonic() - t1) * 1000)
                cur.execute(merge_sql.BATCH_LOG, {
                    "batch_id": batch_id,
                    "started_at": started,
                    "input_rows": input_rows,
                    "customers": per_table["customers"],
                    "products": per_table["products"],
                    "orders": per_table["orders"],
                    "order_items": per_table["order_items"],
                    "dead_letters": dead_total,
                    "min_ts": min(source_ts) if source_ts else None,
                    "max_ts": max(source_ts) if source_ts else None,
                    "stage_ms": stage_ms,
                    "merge_ms": merge_ms,
                })
    finally:
        conn.close()

    batch_df.unpersist()
    log.info("batch %s committed: %s rows %s, stage %sms, merge %sms",
             batch_id, input_rows, per_table, stage_ms, merge_ms)


def main():
    spark = SparkSession.builder.appName("acme-orders-cdc").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(ProgressListener(pg_connect))

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribePattern", TOPIC_PATTERN)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .option("failOnDataLoss", "true")
        .load()
        .select(
            F.col("topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("value").cast("string").alias("value"),
        )
    )

    query = (
        raw.writeStream.queryName("orders_cdc")
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
    log.info("streaming query %s started (trigger %s)", query.id, TRIGGER_INTERVAL)
    query.awaitTermination()


if __name__ == "__main__":
    main()
