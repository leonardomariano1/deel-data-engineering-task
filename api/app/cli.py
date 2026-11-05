"""CLI over the same queries as the API.

    docker compose exec api python -m app.cli open-orders [--date 2026-11-15] [--status PENDING]
    docker compose exec api python -m app.cli top-dates | pending-items | top-customers [--pending-status]
    docker compose exec api python -m app.cli as-of 2026-11-05T01:00:00
    docker compose exec api python -m app.cli timeline 42
    docker compose exec api python -m app.cli freshness | batches | reconcile
"""
import argparse
import json

from . import db, queries, reconcile


def table(rows):
    if not rows:
        print("(no rows)")
        return
    cols = list(rows[0].keys())
    cells = [[("" if r[c] is None else str(r[c])) for c in cols] for r in rows]
    widths = [max(len(c), *(len(row[i]) for row in cells)) for i, c in enumerate(cols)]
    print("  ".join(c.ljust(w) for c, w in zip(cols, widths)))
    print("  ".join("-" * w for w in widths))
    for row in cells:
        print("  ".join(v.ljust(w) for v, w in zip(row, widths)))


def main():
    p = argparse.ArgumentParser(prog="acme-analytics")
    sub = p.add_subparsers(dest="cmd", required=True)
    oo = sub.add_parser("open-orders")
    oo.add_argument("--date")
    oo.add_argument("--status")
    for name in ("top-dates", "pending-items", "freshness", "batches", "reconcile"):
        sub.add_parser(name)
    sub.add_parser("top-customers").add_argument("--pending-status", action="store_true",
                                                 help="rank by the literal PENDING status instead of open orders")
    sub.add_parser("as-of").add_argument("ts")
    sub.add_parser("timeline").add_argument("order_id", type=int)
    a = p.parse_args()

    if a.cmd == "open-orders":
        table(db.fetch(queries.OPEN_ORDERS, {"delivery_date": a.date, "status": a.status}))
    elif a.cmd == "top-dates":
        table(db.fetch(queries.TOP_DELIVERY_DATES))
    elif a.cmd == "pending-items":
        table(db.fetch(queries.PENDING_ITEMS))
    elif a.cmd == "top-customers":
        table(db.fetch(queries.TOP_CUSTOMERS_PENDING_STATUS if a.pending_status else queries.TOP_CUSTOMERS))
    elif a.cmd == "as-of":
        table(db.fetch(queries.OPEN_ORDERS_AS_OF, {"as_of": a.ts}))
    elif a.cmd == "timeline":
        table(db.fetch(queries.ORDER_TIMELINE, {"order_id": a.order_id}))
    elif a.cmd == "freshness":
        table(db.fetch(queries.FRESHNESS))
    elif a.cmd == "batches":
        table(db.fetch(queries.RECENT_BATCHES, {"limit": 15}))
    elif a.cmd == "reconcile":
        print(json.dumps(reconcile.run(), indent=2, default=str))


if __name__ == "__main__":
    main()
