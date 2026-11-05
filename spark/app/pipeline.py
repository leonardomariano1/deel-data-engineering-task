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
from concurrent.futures import ThreadPoolExecutor
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
logging.getLogger("py4j").setLevel(logging.WARNING)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC_PATTERN = os.getenv("TOPIC_PATTERN", r"finance_db\.operations\..*")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "earliest")
MAX_OFFSETS_PER_TRIGGER = int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "200000"))
# split large offset ranges into several tasks (each topic has a single partition)
KAFKA_MIN_PARTITIONS = os.getenv("KAFKA_MIN_PARTITIONS", "8")
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


def stage_table(batch_df, spec, batch_id):
    """Parse one topic of the batch, quarantine bad records and stage the good ones.

    Two Spark jobs per table: one aggregate (valid/dead counts and source time range)
    and the JDBC write. Called concurrently for the four tables.
    """
    parsed = parse_envelope(batch_df, spec.name).persist()
    try:
        st = parsed.agg(
            F.count(F.when(F.col("dq_error").isNull(), 1)).alias("ok"),
            F.count(F.when(F.col("dq_error").isNotNull(), 1)).alias("dead"),
            F.min(F.when(F.col("dq_error").isNull(), F.col("env.source.ts_ms"))).alias("mn"),
            F.max(F.when(F.col("dq_error").isNull(), F.col("env.source.ts_ms"))).alias("mx"),
        ).first()
        if st["dead"]:
            jdbc_append(dead_letters(parsed, spec.name, batch_id), "ops.dead_letter")
            log.warning("batch %s: %s dead letters on %s", batch_id, st["dead"], spec.name)
        if st["ok"]:
            jdbc_append(to_stage(parsed, spec.name), spec.stage_table)
        return spec.name, st["ok"], st["dead"], st["mn"], st["mx"]
    finally:
        parsed.unpersist()


def _ms_to_utc(ms):
    return None if ms is None else datetime.fromtimestamp(ms / 1000, tz=timezone.utc).replace(tzinfo=None)


def process_batch(batch_df, batch_id: int):
    started = datetime.now(timezone.utc)
    t0 = time.monotonic()

    batch_df.persist()
    try:
        counts = {r["topic"]: r["n"] for r in batch_df.groupBy("topic").agg(F.count("*").alias("n")).collect()}
        input_rows = sum(counts.values())
        if input_rows == 0:
            return

        # Stage tables are scratch space for this batch; a replayed batch starts clean.
        conn = pg_connect()
        try:
            with conn, conn.cursor() as cur:
                cur.execute(merge_sql.TRUNCATE_STAGE)
        finally:
            conn.close()

        # The four tables are independent: submit their Spark jobs concurrently so the
        # executors' cores are all busy (one topic alone is a single Kafka partition).
        todo = [spec for spec in TABLES if counts.get(TOPIC_PREFIX + spec.name, 0)]
        per_table = {spec.name: 0 for spec in TABLES}
        dead_total, ts = 0, []
        with ThreadPoolExecutor(max_workers=len(TABLES)) as pool:
            for name, ok, dead, mn, mx in pool.map(lambda sp: stage_table(batch_df, sp, batch_id), todo):
                per_table[name] = ok
                dead_total += dead
                ts += [x for x in (mn, mx) if x is not None]

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
                        "min_ts": _ms_to_utc(min(ts)) if ts else None,
                        "max_ts": _ms_to_utc(max(ts)) if ts else None,
                        "stage_ms": stage_ms,
                        "merge_ms": merge_ms,
                    })
        finally:
            conn.close()
    finally:
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
        .option("minPartitions", KAFKA_MIN_PARTITIONS)
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
