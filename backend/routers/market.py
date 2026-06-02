from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from psycopg2.extensions import connection as PgConn

from backend.database import fetchone_as_dict
from backend.dependencies import get_db
from backend.schemas.market import MarketOverviewResponse

router = APIRouter(prefix="/market", tags=["market"])


@router.get("/overview", response_model=MarketOverviewResponse)
def market_overview(
    date_: date | None = Query(
        None,
        alias="date",
        description="Ngay can lay. Mac dinh: ngay moi nhat.",
    ),
    conn: PgConn = Depends(get_db),
):
    with conn.cursor() as cur:
        if date_ is None:
            cur.execute(
                """
                SELECT
                    trading_date,
                    vnindex_close,
                    vnindex_return,
                    vn30_close,
                    vn30_return,
                    hnx_close,
                    hnx_return,
                    total_volume::bigint AS total_volume,
                    total_market_value AS total_value,
                    advances,
                    declines,
                    unchanged,
                    top_gainers,
                    top_losers
                FROM gold.mart_market_overview
                ORDER BY trading_date DESC
                LIMIT 1
                """
            )
        else:
            cur.execute(
                """
                SELECT
                    trading_date,
                    vnindex_close,
                    vnindex_return,
                    vn30_close,
                    vn30_return,
                    hnx_close,
                    hnx_return,
                    total_volume::bigint AS total_volume,
                    total_market_value AS total_value,
                    advances,
                    declines,
                    unchanged,
                    top_gainers,
                    top_losers
                FROM gold.mart_market_overview
                WHERE trading_date = %s
                """,
                (date_,),
            )
        row = fetchone_as_dict(cur)

    if row is None:
        raise HTTPException(404, "Khong tim thay du lieu thi truong")
    return MarketOverviewResponse(**row)
