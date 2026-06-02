from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query
from psycopg2.extensions import connection as PgConn

from backend.database import fetchall_as_dict
from backend.dependencies import get_db
from backend.schemas.financial import FinancialSummaryRow

router = APIRouter(prefix="/financials", tags=["financials"])


@router.get("/{symbol}", response_model=list[FinancialSummaryRow])
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
                pe_ratio,
                pb_ratio,
                ps_ratio,
                ev_ebit,
                ev_ebitda,
                eps,
                roe,
                roe_trailing,
                roa,
                roa_trailing,
                roce,
                gross_profit_margin,
                net_profit_margin,
                ebit_margin,
                ebitda_margin,
                current_ratio,
                quick_ratio,
                cash_ratio,
                debt_to_equity,
                debt_to_assets,
                liabilities_to_equity,
                liabilities_to_assets,
                revenue_growth,
                gross_profit_growth,
                profit_growth,
                dividend_yield,
                dividend_per_share,
                book_value_per_share,
                cash_flow_per_share,
                beta
            FROM gold.mart_financial_summary
            WHERE ticker = %s
              AND (%s IS NULL OR period_type = %s)
            ORDER BY period DESC
            """,
            (ticker, period_type, period_type),
        )
        rows = fetchall_as_dict(cur)

    if not rows:
        raise HTTPException(404, f"Khong co du lieu tai chinh: {symbol}")
    return [FinancialSummaryRow(**row) for row in rows]
