# Evidence

All captured on 2026-11-05 (UTC) on a laptop, Docker Desktop with 8 GB / 16 vCPU.

| File | What it shows |
|---|---|
| `dashboard.png` | live dashboard after the load tests: the four questions, freshness pill (last commit 5 s ago, e2e 7.2 s) |
| `spark-ui-streaming-statistics.png` | Structured Streaming tab: input rate, process rate, batch duration, operation duration (almost all `addBatch`) while draining a 900k-event burst |
| `spark-ui-jobs.png`, `spark-ui-executors.png` | jobs per micro-batch and the two executors (2 cores, 1 GB each) |
| `spark-master.png` | standalone master with 2 workers and the running application |
| `api-docs.png` | OpenAPI page of the serving API |

## Load test

`scripts/load_test.sql`, run three times on top of the generator (300k, 200k and 100k orders,
4 items each, then status updates). 3.4M CDC events processed in total.

| Metric | Value |
|---|---|
| batches at the 200k cap | stage 14.3 s, merge 7.9 s (average) |
| processed rows/s during load | 9.3k average, 12.4k max |
| share of trigger time in `addBatch` | 99% |
| steady-state end-to-end latency | 7.9 s average, 9.7 s p95 |
| dead letters | 0 |

## Reconciliation

`python -m app.cli reconcile` after each load: every key of the three aggregate tables equals
the same metric computed directly on the OLTP source (90 delivery date x status keys,
27 products, 18 customers).

## Crash in the middle of a batch

1. Started a 100k-order load (about 900k events).
2. `docker kill` on the driver container at 00:51:51 UTC, while batch 24 was running
   (batch 23 had committed at 00:51:32).
3. `docker compose up -d spark-app`: the query restarted from the checkpoint, re-ran batch 24
   with the same offsets (176,848 rows) and continued.

Result after the backlog drained:

```
all_match True attempts 1
dup status events | 0
fact_orders       | 600346
source orders     | 600346
```

No double counting in the delta-maintained aggregates and no duplicate history rows.
