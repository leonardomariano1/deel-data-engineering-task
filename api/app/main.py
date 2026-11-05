"""Serving layer: REST API + live dashboard on top of the mart schema."""
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Query
from fastapi.responses import FileResponse

from . import db, queries, reconcile

app = FastAPI(title="ACME Logistics - open orders analytics", version="1.0.0")
STATIC = Path(__file__).parent / "static"


@app.get("/", include_in_schema=False)
def dashboard():
    return FileResponse(STATIC / "index.html")


@app.get("/api/open-orders", summary="Q1 - open orders by delivery date and status")
def open_orders(delivery_date: Optional[date] = None, status: Optional[str] = None):
    return db.fetch(queries.OPEN_ORDERS, {"delivery_date": delivery_date, "status": status})


@app.get("/api/top-delivery-dates", summary="Q2 - top 3 delivery dates with more open orders (ties kept)")
def top_delivery_dates():
    return db.fetch(queries.TOP_DELIVERY_DATES)


@app.get("/api/pending-items", summary="Q3 - open pending items by product")
def pending_items():
    return db.fetch(queries.PENDING_ITEMS)


@app.get("/api/top-customers", summary="Q4 - top 3 customers with more pending orders (ties kept)")
def top_customers(by: str = Query("open", pattern="^(open|pending_status)$",
                                  description="open = not COMPLETED (default); pending_status = literal PENDING status")):
    return db.fetch(queries.TOP_CUSTOMERS if by == "open" else queries.TOP_CUSTOMERS_PENDING_STATUS)


@app.get("/api/history/open-orders", summary="Open orders by delivery date and status as of a past timestamp (UTC)")
def open_orders_as_of(as_of: datetime = Query(..., description="e.g. 2026-11-05T12:00:00")):
    return db.fetch(queries.OPEN_ORDERS_AS_OF, {"as_of": as_of.replace(tzinfo=None)})


@app.get("/api/orders/{order_id}/timeline", summary="Every status transition of one order")
def order_timeline(order_id: int):
    return db.fetch(queries.ORDER_TIMELINE, {"order_id": order_id})


@app.get("/api/summary", summary="Headline numbers for the dashboard")
def summary():
    return {**db.fetch(queries.TOTALS)[0], **db.fetch(queries.FRESHNESS)[0]}


@app.get("/api/pipeline", summary="Freshness, recent micro-batches and Spark progress metrics")
def pipeline(limit: int = 30):
    return {
        "freshness": db.fetch(queries.FRESHNESS)[0],
        "batches": db.fetch(queries.RECENT_BATCHES, {"limit": limit}),
        "progress": db.fetch(queries.RECENT_PROGRESS, {"limit": limit}),
    }


@app.get("/api/reconcile", summary="Compare marts with the same metrics computed on the source DB")
def run_reconcile():
    return reconcile.run()
