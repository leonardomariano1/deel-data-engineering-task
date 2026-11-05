# Evidence

Captured on 2026-11-05 (UTC) from a **fresh clone of the GitHub repository**, started with
`docker compose up -d --build` on empty volumes, on a laptop with Docker Desktop (8 GB VM).
Data: the provided pg_cron generator plus `scripts/load_test.sql` with 300k and then 100k
orders (4 items each, ~40% status changes, 1% deletes).

| File | What it shows |
|---|---|
| `dashboard.png` | live dashboard at 02:14 UTC: the four questions, freshness pill (last commit 7 s before, e2e 8.3 s) |
| `spark-ui-streaming-statistics.png` | Structured Streaming tab for the run started after the forced replay: the replayed batch, the next big batch, then steady state. Operation duration is almost all `addBatch` |
| `spark-ui-jobs.png`, `spark-ui-executors.png` | jobs per micro-batch and the two executors (2 cores, 1 GB each) |
| `spark-master.png` | standalone master with 2 workers and the running application |
| `api-docs.png` | OpenAPI page of the serving API |

## Metrics (from `ops.batch_log` and `ops.streaming_progress`)

| Metric | Value |
|---|---|
| Kafka records consumed | 3,565,700 (equal to the sum of the four topics' end offsets) |
| 200k-event batches, warm driver (13 batches) | staging 4.3 s, merge 5.2 s on average |
| same, previous version (staging one table at a time) | staging 14.3 s, merge 7.9 s |
| processed rows/s on big batches | 18.3k average, 24.3k max |
| share of trigger time in `addBatch` | 98% |
| steady-state latency, newest event of the batch (8 batches) | 6.9 s average, 7.5 s p95 |
| steady-state latency, oldest event of the batch | 7.4 s average, 8.0 s p95 |
| steady-state event rate from the generator | 3.6 events/s |
| dead letters | 0 |

## Replay of a batch that was already committed

The hard case for exactly-once results: Postgres has committed the batch, the Spark
checkpoint has not, so Spark runs the same batch again on top of data that already
contains it.

1. Started a 100k-order load (~540k events). Batch 23 (188,419 events) committed in
   Postgres at 02:02:26.9 and in the checkpoint; batch 24 had been planned.
2. `docker kill` on the driver at 02:02:28.
3. Removed `commits/23` and `offsets/24` from the checkpoint volume, so Spark considers
   batch 23 planned but not finished. (Removing only `commits/23` is not enough: Spark
   then re-runs only the latest planned batch.)
4. `docker compose up -d spark-app`. The driver re-ran batch 23 with the same offsets
   (188,419 events, committed again at 02:03:20) and continued with 24, 25, ...

After the backlog drained:

```
reconcile: all_match True, attempts 1   (90 delivery date x status keys, 28 products, 18 customers)
duplicate history rows (order_id, source_lsn): 0
fact_orders live = 495,121 = source orders
fact_order_items live = 1,983,630 = source order_items
```

The merge tests (`spark/tests/test_merge_sql.py`) assert the same property in isolation:
applying a committed batch a second time leaves marts, history and dimensions unchanged.

## Duplicates from Debezium

During the first 300k-order load, Kafka Connect was OOM-killed by Docker (default heap up to
2 GB on an 8 GB VM). After it restarted (heap now capped) it resumed from its last committed
offset and re-published events. The `order_items` topic holds 2,743,349 records for
2,024,404 source changes (2,004,316 inserts + 20,088 deletes), so about 719k records were
duplicates. The version guards and the delta maintenance absorbed them: the reconciliation
above ran after that recovery.

## Reconciliation

`python -m app.cli reconcile` recomputes the three aggregates on the OLTP source and compares
every key with the marts. It matched after each load, after the Debezium recovery and after
the forced replay.
