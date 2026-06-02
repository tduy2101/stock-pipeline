from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, Query
from psycopg2.extensions import connection as PgConn

from backend.database import fetchall_as_dict
from backend.dependencies import get_db
from backend.schemas.board import ForeignFlowRow, PriceBoardRow

router = APIRouter(prefix="/board", tags=["board"])


@router.get("/{symbol}", response_model=list[PriceBoardRow])
def get_price_board(
    symbol: str,
    from_date: date | None = Query(None, alias="from"),
    to_date: date | None = Query(None, alias="to"),
    page_size: int = Query(30, ge=1, le=200),
    conn: PgConn = Depends(get_db),
):
    ticker = symbol.upper()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                symbol,
                trading_date,
                exchange,
                ceiling_price,
                floor_price,
                reference_price,
                close_price,
                open_price,
                high_price,
                low_price,
                average_price,
                percent_change,
                price_change,
                volume_accumulated,
                total_value,
                bid_price_1,
                bid_vol_1,
                bid_price_2,
                bid_vol_2,
                bid_price_3,
                bid_vol_3,
                ask_price_1,
                ask_vol_1,
                ask_price_2,
                ask_vol_2,
                ask_price_3,
                ask_vol_3,
                foreign_buy_volume,
                foreign_sell_volume,
                foreign_net_volume,
                foreign_room,
                spread_pct,
                snapshot_at
            FROM gold.mart_price_board
            WHERE symbol = %s
              AND (%s IS NULL OR trading_date >= %s)
              AND (%s IS NULL OR trading_date <= %s)
            ORDER BY trading_date DESC
            LIMIT %s
            """,
            (ticker, from_date, from_date, to_date, to_date, page_size),
        )
        rows = fetchall_as_dict(cur)

    return [PriceBoardRow(**row) for row in rows]


@router.get("/{symbol}/foreign-flow", response_model=list[ForeignFlowRow])
def get_foreign_flow(
    symbol: str,
    from_date: date | None = Query(None, alias="from"),
    to_date: date | None = Query(None, alias="to"),
    days: int = Query(30, ge=1, le=252),
    conn: PgConn = Depends(get_db),
):
    ticker = symbol.upper()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                trading_date,
                foreign_buy_volume,
                foreign_sell_volume,
                foreign_net_volume,
                foreign_room
            FROM gold.mart_price_board
            WHERE symbol = %s
              AND (%s IS NULL OR trading_date >= %s)
              AND (%s IS NULL OR trading_date <= %s)
            ORDER BY trading_date DESC
            LIMIT %s
            """,
            (ticker, from_date, from_date, to_date, to_date, days),
        )
        rows = fetchall_as_dict(cur)

    return [ForeignFlowRow(**row) for row in rows]
