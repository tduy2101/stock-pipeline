from __future__ import annotations

from fastapi import APIRouter, Depends
from psycopg2.extensions import connection as PgConn

from backend.dependencies import get_db

router = APIRouter(tags=["health"])


@router.get("/health")
def health_check(conn: PgConn = Depends(get_db)):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        return {"status": "ok", "db": "connected"}
    except Exception as exc:
        return {"status": "error", "db": str(exc)}
