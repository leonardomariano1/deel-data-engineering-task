# ACME Delivery Services: near-real-time open orders analytics

Spark Structured Streaming consumes the Debezium CDC topics of the orders system, keeps
a dimensional model (SCD2 dimensions, current-state facts and a full change history) in
an analytical Postgres, and maintains the four operational metrics incrementally, in the
same transaction as the facts. Stakeholders read them through a live dashboard, a REST
API, a CLI or any SQL client, a few seconds after the change happens at the source.

![Architecture](docs/architecture.png)

| Must have | How it is covered |
|---|---|
| Historical + current state | Debezium initial snapshot (`op=r`) + change stream land in the same model. Current state in `core.fact_orders` / `fact_order_items`, every status, delivery date or customer change in `core.fact_order_status_events`, every customer/product version in SCD2 dimensions. `mart.open_orders_as_of(ts)` answers Q1 for any past instant. |
| Dimensional model | `core` schema: `dim_date`, `dim_order_status`, `dim_customer` (SCD2), `dim_product` (SCD2), `fact_orders` (accumulating snapshot), `fact_order_items`, `fact_order_status_events` (transactional). `mart` schema: aggregates + views per question. |
| Available as soon as possible | 5 s trigger. Measured source-commit to mart-commit latency in steady state: **6.9 s average, 7.5 s p95** for the newest event of a batch, **7.4 s / 8.0 s** for the oldest. |

## Run it

Requirements: Docker Desktop with about 6 GB of memory for the engine.

```bash
docker compose up -d --build
```

First start takes a few minutes (image build + Debezium snapshot).

**About the data.** The provided pg_cron generator keeps only a handful of orders open (about
20) and, because of a bug described under *Findings*, those orders have no items, so with the
generator alone Q3 is correct but nearly empty. To see the platform with volume, run the
optional load test after the stack is up (about 1.6M CDC events, drained in ~2 minutes):

```bash
docker compose exec -T transactions-db psql -U finance_db_user -d finance_db \
  -v orders=300000 -f - < scripts/load_test.sql
```

All numbers and screenshots in this repository come from the generator plus this script.

Then:

| What | Where |
|---|---|
| Live dashboard | http://localhost:8010 |
| REST API (OpenAPI) | http://localhost:8010/docs |
| Spark driver UI, Structured Streaming tab | http://localhost:4040/StreamingQuery/ |
| Spark master / workers | http://localhost:8090, :8091, :8092 |
| Spark metrics (Prometheus format) | http://localhost:4040/metrics/prometheus |
| Analytics DB | `psql postgresql://analytics:analytics@localhost:5433/analytics` |
| Source DB (provided) | `psql postgresql://finance_db_user:1234@localhost:5432/finance_db` |

Host ports 8080 and 8000 were left free on purpose; they are commonly taken by other
local services.

### The four business questions

```bash
# Q1 open orders by delivery date and status
curl 'localhost:8010/api/open-orders'                 # filters: ?delivery_date=2026-11-15&status=PENDING
# Q2 top 3 delivery dates with more open orders
curl 'localhost:8010/api/top-delivery-dates'
# Q3 open pending items by product
curl 'localhost:8010/api/pending-items'
# Q4 top 3 customers with more pending orders
curl 'localhost:8010/api/top-customers'

# history: same as Q1 at any past instant, and the life of one order
curl 'localhost:8010/api/history/open-orders?as_of=2026-11-05T00:50:00'
curl 'localhost:8010/api/orders/42/timeline'
```

CLI (same SQL as the API):

```bash
docker compose exec api python -m app.cli open-orders --status PENDING
docker compose exec api python -m app.cli top-dates
docker compose exec api python -m app.cli pending-items
docker compose exec api python -m app.cli top-customers
docker compose exec api python -m app.cli as-of 2026-11-05T00:50:00
docker compose exec api python -m app.cli freshness
docker compose exec api python -m app.cli reconcile
```

SQL:

```sql
SELECT * FROM mart.v_open_orders_by_delivery_date_status;
SELECT * FROM mart.v_top3_delivery_dates;
SELECT * FROM mart.v_pending_items_by_product;
SELECT * FROM mart.v_top3_customers;
SELECT * FROM mart.open_orders_as_of('2026-11-05 00:50:00');
```

### Tests and verification

```bash
# 11 tests: Debezium parsing (local Spark) + the transactional merge on a throwaway
# Postgres database (snapshot, replay of a committed batch, item before its order,
# stale event, delivery date change, delete, SCD2). Needs analytics-db running.
docker compose run --rm --no-deps -v ./analytics-db/init:/schema:ro spark-app \
  python3 -m pytest -q tests

# reconciliation: recompute every metric on the OLTP source and diff against the marts
docker compose exec api python -m app.cli reconcile
```

### Stop / reset

```bash
docker compose stop          # keep everything, resume later from the checkpoint
docker compose down -v       # wipe source, Kafka, checkpoint and analytics data
```

Kafka, ZooKeeper, the Spark checkpoint and both databases live in named volumes, so
`down` + `up` resumes where it stopped. Wiping only some of them (for example the analytics
database without the checkpoint) leaves the job and the marts out of sync; use `down -v`.

## Business definitions (assumptions)

* **Open order**: status is not `COMPLETED` and the order was not deleted. The rule lives in
  `core.dim_order_status.is_open`, not in code, so the business can change it.
* **Pending orders** (Q4): same as open orders, because Q3 already uses "pending" for items of
  orders that are not completed, and PROCESSING / REPROCESSING orders still need warehouse work.
  If the business means the literal `PENDING` status, the same mart answers it:
  `/api/top-customers?by=pending_status`, `top-customers --pending-status`,
  `mart.v_top3_customers_pending_status`.
* **Pending items** (Q3): sum of `quantity` of the items that belong to open orders, plus the
  number of lines.
* **Top 3**: `RANK()`, so ties are shown instead of being cut arbitrarily.
* Source timestamps are wall-clock UTC (the source container runs in UTC).

## Design

### Pipeline (`spark/app`)

One streaming query subscribes to the four topics (`subscribePattern`): one checkpoint and one
Postgres transaction cover all tables. An order and its items are not guaranteed to land in
the same micro-batch (topics are produced independently and `maxOffsetsPerTrigger` splits the
limit across them); the merge is correct for either order of arrival. Each batch, inside
`foreachBatch`:

1. **Parse and validate** the Debezium envelope per topic (`transform.py`). The row image is
   `after`, or `before` for deletes. Malformed JSON, tombstones, unknown `op`, missing
   primary key or LSN go to `ops.dead_letter` with the raw payload, never silently dropped.
2. **Stage** typed rows into unlogged `stage.*` tables over JDBC. Each topic has a single
   partition, so the four tables are staged concurrently (driver thread pool) and
   `minPartitions=8` splits large offset ranges into several tasks. Every version is kept;
   ordering is resolved by `(source_lsn, kafka_offset)`.
3. **Apply** everything in **one Postgres transaction** from the driver (`merge_sql.py`):
   * capture the current state of the orders/items the batch touches,
   * SCD2 merge of customers and products (a new version only when a tracked attribute changes),
   * upsert of the facts guarded by `(source_lsn, kafka_offset)`: a replayed or older
     event never overwrites a newer one,
   * insert into the history fact of every creation, deletion and change of status, delivery
     date or customer, unique per source change `(order_id, LSN)`,
   * capture the new state and apply **deltas** (new contribution minus old contribution)
     to the three aggregate tables,
   * write the batch log with the end-to-end latency.

Why this shape:

* **Correctness under replays.** Spark writes the batch commit to the checkpoint after
  `foreachBatch` returns, so a crash between the Postgres commit and the checkpoint commit
  replays a batch that is already applied. Debezium is also at-least-once. Every statement is
  idempotent and the deltas are computed from the actual before/after state, so a replay or
  a duplicate contributes zero. Covered by the merge tests and verified live: a committed
  188k-event batch was forced to replay, and ~719k events re-published by Debezium after a
  crash were absorbed, with the reconciliation still exact (see *Evidence*).
* **Consistency for readers.** Facts and marts commit together, and Postgres MVCC gives each
  query a consistent snapshot: the dashboard never shows an aggregate that disagrees with the
  facts behind it.
* **Cost proportional to the change, not to history.** Deltas touch only the keys in the
  batch. With ~500k orders and 2M items, the merge of a 200k-event batch takes about 5 s,
  and a steady-state batch about 40 ms.
* **Late or out-of-order data.** Items that arrive before their order contribute nothing until
  the order arrives; at that point the order change pulls its items into the delta. Version
  guards make arrival order irrelevant for the facts.
* **Stateless Spark.** No state store, watermarks or stream-stream joins: the "join with
  historical data" happens against the persisted model, which survives Kafka retention
  and checkpoint loss. Executors size by batch volume only.

### Data model (`analytics-db/init/01_schema.sql`)

| Table | Grain | Notes |
|---|---|---|
| `core.dim_date` | day | 2020 to 2035, ISO weekday, weekend flag |
| `core.dim_order_status` | status | carries the `is_open` business rule |
| `core.dim_customer` | customer version | SCD2: `valid_from`, `valid_to`, `is_current` |
| `core.dim_product` | product version | SCD2, keeps price history |
| `core.fact_orders` | order (latest state) | accumulating snapshot, date keys to `dim_date` |
| `core.fact_order_items` | order line (latest state) | source column `quanity` renamed `quantity` |
| `core.fact_order_status_events` | order change | history: snapshot, insert, delete, and updates of status, delivery date or customer |
| `mart.open_orders_by_delivery_date_status` | delivery date x status | Q1, Q2 |
| `mart.pending_items_by_product` | product | Q3 |
| `mart.pending_orders_by_customer` | customer | Q4 |

Facts reference dimensions by durable (natural) key; the view layer joins the current
version (`is_current`) and point-in-time joins use `valid_from/valid_to`. Resolving surrogate
keys at load time in a stream would force dimension-before-fact ordering for no gain in
this use case.

### Serving (`api/app`)

FastAPI + psycopg pool reading only the `mart` and `ops` schemas. The dashboard polls every
5 s. `/api/reconcile` and the `reconcile` CLI command recompute every metric on the source
database and diff it against the marts (with a short retry, since changes are always in
flight).

## Cluster sizing

Local deployment: Spark standalone, 1 master, 2 workers with 2 cores / 1.5 GB each;
the application uses 2 executors x 2 cores x 1 GB (`spark.cores.max=4`), driver 1 GB in
client mode.

Variables taken into account:

| Variable | Value here | Decision |
|---|---|---|
| Source parallelism | 4 topics x 1 partition | 4 cores. A single partition per topic would keep 3 cores idle, so tables are staged concurrently and `minPartitions=8` splits ranges. `spark.sql.shuffle.partitions=4` (the default 200 creates 200 near-empty tasks per micro-batch). |
| Event rate | 3.6 events/s steady (pg_cron generator); bursts of 1.6M events in the load test | `maxOffsetsPerTrigger=200000` caps batch size so a burst or a re-snapshot never produces a giant batch. |
| Event size | 0.5 to 0.8 KB JSON | 200k events is about 150 MB raw, cached once per batch: 1 GB executors leave headroom. |
| Latency target | seconds | 5 s processing-time trigger; continuous mode is not compatible with `foreachBatch`. |
| State | none in Spark | memory does not grow with history or with the number of keys. |
| Sink capacity | Postgres merge | after the staging fix the merge is the larger half of a big batch (see metrics); more Spark cores alone would not help. |
| Memory of the whole stack | 8 GB Docker VM | JVM heaps capped (Kafka 512 MB, Kafka Connect 768 MB): with defaults, Connect was OOM-killed during a load test. |

### Spark metrics used

Collected by a `StreamingQueryListener` into `ops.streaming_progress`, by the job itself into
`ops.batch_log`, and exposed by the Prometheus servlet.

Numbers from the clean run (fresh clone, generator + 300k and 100k-order load tests).
"Big batch" = a 200k-event batch, excluding the first batch after a driver restart (cold JVM).

| Metric | Measured | What it told us |
|---|---|---|
| `durationMs.addBatch / triggerExecution` | 98% | the time is in `foreachBatch` (staging + merge), not in Kafka reads or planning |
| `stage_ms` vs `merge_ms` (batch log), before the fix | 14.3 s / 7.9 s per big batch | staging used one core: 4 single-partition topics staged one after the other |
| `stage_ms` vs `merge_ms`, after the fix | **4.3 s / 5.2 s** per big batch | staging 3.3x faster; the Postgres merge is now the larger half |
| `processedRowsPerSecond` | 9.3k avg before, **18.3k avg / 24.3k max** after | about 4.5k events/s per core end to end, sink included |
| `inputRowsPerSecond` vs `processedRowsPerSecond` | processed above input after a burst | the job catches up, no growing backlog |
| end-to-end latency, steady (batch log) | newest event 6.9 s avg / 7.5 s p95; oldest 7.4 s / 8.0 s | dominated by the 5 s trigger plus ~2 s of staging per batch |
| end-to-end latency, burst | 16 to 30 s while draining 1.6M events normally; up to 12 min while recovering the backlog left by the Kafka Connect crash | bursts drain in about 2 minutes |
| `numInputRows` per batch | at most 200k | backpressure working |

### Scaling to production volumes

* Throughput per core is about 4.5k events/s with this sink. For a peak of R events/s,
  plan `R / 4500 x 2` cores (2x headroom) and **at least as many Kafka partitions as cores**
  (or `minPartitions` on the source); partition by primary key so per-key order holds.
* Run on Kubernetes or a managed Spark service with the checkpoint on object storage, the
  driver in cluster mode with restart policy, and dynamic allocation off for streaming
  (predictable latency).
* The sink scales separately: replace JDBC inserts with `COPY`, partition the facts by
  `delivery_date`, or move core/history to a lakehouse table format (Delta/Iceberg `MERGE`)
  and keep Postgres only for the small mart tables that the API reads.

## Evidence

See [`docs/evidence`](docs/evidence/README.md): Spark UI screenshots, dashboard, the load
test numbers and the crash/replay test.

## Incidents found while testing (and fixed)

* **Kafka Connect OOM-killed** during a 1.6M-event load on an 8 GB Docker VM (default heap up
  to 2 GB). CDC stopped silently. Fixed by capping the JVM heaps. When Connect came back it
  resumed from its last committed offset and re-published ~719k item events; the version
  guards absorbed them and the reconciliation stayed exact.
* **Driver orphaned by a master restart.** Restarting the standalone master leaves the driver
  without executors; it waits instead of failing. Restarting `spark-app` recovers from the
  checkpoint. See *What I would do next*.
* **Load-test script.** A `NOT EXISTS` over `order_items` (no index on `order_id`) became a
  nested loop on a 1.2M-row insert; the script now selects its rows by ID range.

## Findings in the source data

* `order_items.quanity` is misspelled in the DDL (the ERD says `quantity`). Renamed in the model.
* `products.unity_price` is an unconstrained `NUMERIC`, which Debezium encodes as a base64
  struct by default. The connector now emits decimals as strings.
* The generator (`generate_orders`) attaches new items with
  `SELECT order_id ... WHERE customer_id = ? AND order_date = CURRENT_DATE LIMIT 1`, without
  a status filter or ordering. Items always go to the customer's first order of the day,
  so orders created later that day (the ones that stay open) have **no items**. With only the
  generator running, Q3 is correct but nearly empty; the reconciliation confirms the source
  shows the same. The load test script creates orders with items to exercise Q3.
* Orders from previous days are never updated again by the generator, so they stay open
  forever.
* The generator never deletes, so the load test deletes 1% of its orders to exercise that path.

## What I would do next

* Speed up the sink: `COPY` instead of JDBC inserts for staging, and partition the facts so the
  merge (now the larger half of a big batch) touches smaller indexes.
* Avro + Schema Registry instead of schemaless JSON, with schema evolution checks in CI.
* Debezium incremental snapshots (signal table) to backfill or repair a single table without
  restarting the connector.
* Data quality: alert on dead letters, orders without items, and reconciliation drift;
  publish freshness as an SLO.
* Prometheus + Grafana for the Spark and Postgres metrics, alerting on batch duration above
  the trigger interval for N batches.
* Make the driver exit when it loses all executors (e.g. after a standalone master restart)
  so the container restart policy brings it back; today it waits for new executors.
* Partition `fact_order_status_events` by month and archive cold partitions.
* Integration test in CI: compose up, run the load script, assert reconciliation passes.

## Repository layout

```
analytics-db/init/01_schema.sql   dimensional model, marts, ops tables
spark/Dockerfile                  one image for master, workers and driver
spark/conf/                       spark-defaults.conf, metrics.properties
spark/app/pipeline.py             streaming query + foreachBatch
spark/app/transform.py            Debezium parsing and validation (pure functions)
spark/app/merge_sql.py            transactional merge + delta maintenance
spark/app/progress_listener.py    StreamingQueryListener -> ops.streaming_progress
spark/tests/                      parsing tests (local Spark) + merge tests (real Postgres)
api/app/                          FastAPI, dashboard, CLI, reconciliation
scripts/load_test.sql             1.6M-event load test
docs/                             architecture diagram, evidence, presentation
```

The source stack (Postgres with pg_cron generator, Kafka, Debezium) is the one provided in the
original repository; its topics are `finance_db.operations.{customers,products,orders,order_items}`.
