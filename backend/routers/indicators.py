from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from psycopg2.extensions import connection as PgConn

from backend.config import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE
from backend.database import fetchall_as_dict
from backend.dependencies import get_db
from backend.schemas.common import PaginatedResponse
from backend.schemas.price import IndicatorRow

router = APIRouter(prefix="/indicators", tags=["indicators"])


@router.get("/{symbol}", response_model=PaginatedResponse[IndicatorRow])
def get_indicators(
    symbol: str,
    from_date: date | None = Query(None, alias="from"),
    to_date: date | None = Query(None, alias="to"),
    page: int = Query(1, ge=1),
    page_size: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    conn: PgConn = Depends(get_db),
):
    ticker = symbol.upper()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM gold.mart_stock_daily
            WHERE ticker = %s
              AND (%s IS NULL OR trading_date >= %s)
              AND (%s IS NULL OR trading_date <= %s)
            """,
            (ticker, from_date, from_date, to_date, to_date),
        )
        total = cur.fetchone()[0]

        if total == 0:
            raise HTTPException(404, f"Khong co du lieu chi bao: {symbol}")

        offset = (page - 1) * page_size
        cur.execute(
            """
            WITH enriched AS (
                SELECT
                    ticker,
                    trading_date,
                    close,
                    ma7,
                    ma20,
                    ma50,
                    rsi14,
                    macd_line,
                    macd_signal,
                    macd_hist,
                    bb_upper,
                    bb_middle,
                    bb_lower,
                    volume_ma20,
                    obv,
                    stddev_pop(daily_return) OVER (
                        PARTITION BY ticker
                        ORDER BY trading_date
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS volatility_20d
                FROM gold.mart_stock_daily
                WHERE ticker = %s
            )
            SELECT *
            FROM enriched
            WHERE (%s IS NULL OR trading_date >= %s)
              AND (%s IS NULL OR trading_date <= %s)
            ORDER BY trading_date DESC
            LIMIT %s OFFSET %s
            """,
            (ticker, from_date, from_date, to_date, to_date, page_size, offset),
        )
        rows = fetchall_as_dict(cur)

    return PaginatedResponse(
        data=[IndicatorRow(**row) for row in rows],
        total=total,
        page=page,
        page_size=page_size,
        has_more=(offset + len(rows)) < total,
    )
