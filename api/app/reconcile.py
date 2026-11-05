"""Compares the incrementally maintained marts with the same metrics computed
directly on the OLTP source. Small transient differences are expected while a
change is in flight (latency is seconds), so we retry before reporting a diff."""
import time

from . import db, queries


def _index(rows):
    out = {}
    for r in rows:
        if isinstance(r[1], str):           # (delivery_date, status, count)
            out[(r[0], r[1])] = (int(r[2]),)
        else:                               # (key, a, b)
            out[(r[0],)] = (int(r[1]), int(r[2]))
    return out


def run_once():
    report = {}
    for name in queries.SOURCE:
        src = _index(db.source_rows(queries.SOURCE[name]))
        mart = _index(db.analytics_rows(queries.MART[name]))
        diffs = [{"key": list(k), "source": src.get(k), "mart": mart.get(k)}
                 for k in sorted(set(src) | set(mart)) if src.get(k) != mart.get(k)]
        report[name] = {"source_keys": len(src), "mart_keys": len(mart),
                        "matches": not diffs, "diffs": diffs[:20]}
    return report


def run(retries=3, wait_s=4):
    for attempt in range(1, retries + 1):
        report = run_once()
        if all(v["matches"] for v in report.values()):
            break
        if attempt < retries:
            time.sleep(wait_s)
    return {"attempts": attempt, "all_match": all(v["matches"] for v in report.values()), "checks": report}
