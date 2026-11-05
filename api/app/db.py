import os

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

ANALYTICS_DSN = os.getenv("ANALYTICS_DSN", "postgresql://analytics:analytics@localhost:5433/analytics")
SOURCE_DSN = os.getenv("SOURCE_DSN", "postgresql://finance_db_user:1234@localhost:5432/finance_db")

_pool = None


def pool() -> ConnectionPool:
    global _pool
    if _pool is None:
        _pool = ConnectionPool(ANALYTICS_DSN, min_size=1, max_size=8,
                               kwargs={"row_factory": dict_row, "autocommit": True}, open=True)
    return _pool


def fetch(sql, params=None):
    with pool().connection() as conn:
        return conn.execute(sql, params or {}).fetchall()


def source_rows(sql):
    with psycopg.connect(SOURCE_DSN) as conn:
        return conn.execute(sql).fetchall()


def analytics_rows(sql):
    with psycopg.connect(ANALYTICS_DSN) as conn:
        return conn.execute(sql).fetchall()
