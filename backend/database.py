from __future__ import annotations

from collections.abc import Iterator

import psycopg2
import psycopg2.pool

from backend.config import DATABASE_URL

_pool: psycopg2.pool.SimpleConnectionPool | None = None


def get_pool() -> psycopg2.pool.SimpleConnectionPool:
    global _pool
    if _pool is None:
        _pool = psycopg2.pool.SimpleConnectionPool(
            minconn=2,
            maxconn=10,
            dsn=DATABASE_URL,
        )
    return _pool


def close_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.closeall()
        _pool = None


def get_db() -> Iterator[psycopg2.extensions.connection]:
    pool = get_pool()
    conn = pool.getconn()
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


def fetchall_as_dict(cur) -> list[dict]:
    cols = [desc[0] for desc in cur.description]
    return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]


def fetchone_as_dict(cur) -> dict | None:
    row = cur.fetchone()
    if row is None:
        return None
    cols = [desc[0] for desc in cur.description]
    return dict(zip(cols, row, strict=True))
