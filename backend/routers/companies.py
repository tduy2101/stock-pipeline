from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from psycopg2.extensions import connection as PgConn

from backend.database import fetchone_as_dict
from backend.dependencies import get_db
from backend.schemas.company import CompanyProfileResponse

router = APIRouter(prefix="/companies", tags=["companies"])


@router.get("/{symbol}", response_model=CompanyProfileResponse)
def get_company(symbol: str, conn: PgConn = Depends(get_db)):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                cp.ticker,
                cp.symbol,
                cp.exchange,
                cp.company_name,
                cp.short_name,
                cp.industry,
                cp.sector,
                cp.charter_capital,
                cp.established_year,
                cp.listed_date,
                cp.website,
                cp.description,
                cp.organ_name,
                cp.en_organ_name,
                cp.latest_close,
                cp.latest_trading_date,
                cp.high_52w,
                cp.low_52w,
                cp.avg_volume_20d,
                cp.pe_ratio,
                cp.pb_ratio,
                cp.eps,
                cp.roe,
                cp.roa,
                coalesce(td.has_full_profile, true) as has_full_profile,
                coalesce(td.has_price, cp.latest_close is not null) as has_price,
                coalesce(td.has_news, false) as has_news,
                coalesce(td.has_bctc, false) as has_bctc
            FROM gold.mart_company_profile AS cp
            LEFT JOIN gold.mart_ticker_directory AS td
              ON cp.ticker = td.ticker
            WHERE cp.ticker = UPPER(%s)
            """,
            (symbol,),
        )
        row = fetchone_as_dict(cur)

    if row is None:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    ticker,
                    ticker as symbol,
                    exchange,
                    null::text as company_name,
                    null::text as short_name,
                    null::text as industry,
                    null::text as sector,
                    null::bigint as charter_capital,
                    null::integer as established_year,
                    null::date as listed_date,
                    null::text as website,
                    null::text as description,
                    organ_name,
                    en_organ_name,
                    null::double precision as latest_close,
                    latest_trading_date,
                    null::double precision as high_52w,
                    null::double precision as low_52w,
                    null::double precision as avg_volume_20d,
                    null::double precision as pe_ratio,
                    null::double precision as pb_ratio,
                    null::double precision as eps,
                    null::double precision as roe,
                    null::double precision as roa,
                    has_full_profile,
                    has_price,
                    has_news,
                    has_bctc
                FROM gold.mart_ticker_directory
                WHERE ticker = UPPER(%s)
                """,
                (symbol,),
            )
            row = fetchone_as_dict(cur)

    if row is None:
        raise HTTPException(404, f"Khong tim thay ticker: {symbol}")
    return CompanyProfileResponse(**row)
