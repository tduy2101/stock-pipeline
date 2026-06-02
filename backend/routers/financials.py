from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from psycopg2.extensions import connection as PgConn

from backend.database import fetchall_as_dict
from backend.dependencies import get_db
from backend.schemas.financial import FinancialRatioRow

router = APIRouter(prefix="/financials", tags=["financials"])


@router.get("/{symbol}", response_model=list[FinancialRatioRow])
def get_financials(
    symbol: str,
    period_type: str | None = Query(
        None,
        description="'quarter' hoac 'annual'. Mac dinh: tat ca.",
    ),
    conn: PgConn = Depends(get_db),
):
    ticker = symbol.upper()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                ticker,
                period,
                period_type,
                year,
                quarter,
                item_code,
                item_name,
                value
            FROM gold.stg_financial_ratio
            WHERE ticker = %s
              AND (%s IS NULL OR period_type = %s)
              AND value IS NOT NULL
            ORDER BY period DESC, item_code
            """,
            (ticker, period_type, period_type),
        )
        rows = fetchall_as_dict(cur)

    if not rows:
        raise HTTPException(404, f"Khong co du lieu tai chinh: {symbol}")
    return [FinancialRatioRow(**row) for row in rows]
