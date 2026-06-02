from __future__ import annotations

from fastapi import APIRouter, Depends
from psycopg2.extensions import connection as PgConn

from backend.database import fetchall_as_dict
from backend.dependencies import get_db
from backend.schemas.ticker import TickerItem, TickerListResponse

router = APIRouter(prefix="/tickers", tags=["tickers"])


@router.get("", response_model=TickerListResponse)
def list_tickers(conn: PgConn = Depends(get_db)):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                ticker,
                exchange,
                organ_name,
                has_full_profile,
                has_price,
                has_news,
                has_bctc,
                news_count,
                bctc_doc_count
            FROM gold.mart_ticker_directory
            ORDER BY ticker
            """
        )
        rows = fetchall_as_dict(cur)

    return TickerListResponse(
        tickers=[TickerItem(**row) for row in rows],
        total=len(rows),
    )
