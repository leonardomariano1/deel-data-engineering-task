"""Persists StreamingQueryProgress to ops.streaming_progress so the key Spark
metrics (input rate, processing rate, batch duration breakdown, source lag)
are queryable next to the business data and shown on the dashboard."""
import json
import logging

from pyspark.sql.streaming import StreamingQueryListener

log = logging.getLogger("orders-cdc.listener")

INSERT = """
INSERT INTO ops.streaming_progress (
    query_name, run_id, batch_id, progress_ts, num_input_rows,
    input_rows_per_second, processed_rows_per_second, trigger_execution_ms,
    add_batch_ms, get_batch_ms, latest_offset_ms, wal_commit_ms, sources_json)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


class ProgressListener(StreamingQueryListener):
    def __init__(self, connect):
        self._connect = connect

    def onQueryStarted(self, event):
        log.info("query started id=%s run=%s", event.id, event.runId)

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        log.warning("query terminated id=%s exception=%s", event.id, event.exception)

    def onQueryProgress(self, event):
        p = event.progress
        d = p.durationMs or {}
        try:
            conn = self._connect()
            with conn, conn.cursor() as cur:
                cur.execute(INSERT, (
                    p.name, str(p.runId), p.batchId, p.timestamp, p.numInputRows,
                    _num(p.inputRowsPerSecond), _num(p.processedRowsPerSecond),
                    d.get("triggerExecution"), d.get("addBatch"), d.get("getBatch"),
                    d.get("latestOffset"), d.get("walCommit"),
                    json.dumps([json.loads(s.json) for s in p.sources]),
                ))
            conn.close()
        except Exception:  # metrics must never break the pipeline
            log.exception("could not persist streaming progress")


def _num(x):
    try:
        x = float(x)
        return x if x == x and x not in (float("inf"), float("-inf")) else None
    except (TypeError, ValueError):
        return None
