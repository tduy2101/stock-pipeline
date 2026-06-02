from __future__ import annotations

from datetime import date

from pydantic import BaseModel


class CompanyProfileResponse(BaseModel):
    ticker: str
    symbol: str | None
    exchange: str | None
    company_name: str | None
    short_name: str | None
    industry: str | None
    sector: str | None
    charter_capital: int | None
    established_year: int | None
    listed_date: date | None
    website: str | None
    description: str | None
    organ_name: str | None
    en_organ_name: str | None
    latest_close: float | None
    latest_trading_date: date | None
    high_52w: float | None
    low_52w: float | None
    avg_volume_20d: float | None
    pe_ratio: float | None
    pb_ratio: float | None
    eps: float | None
    roe: float | None
    roa: float | None
    has_full_profile: bool = True
    has_price: bool = False
    has_news: bool = False
    has_bctc: bool = False
