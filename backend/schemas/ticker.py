from __future__ import annotations

from pydantic import BaseModel


class TickerItem(BaseModel):
    ticker: str
    exchange: str | None
    organ_name: str | None
    has_full_profile: bool = False
    has_price: bool = False
    has_news: bool = False
    has_bctc: bool = False
    news_count: int = 0
    bctc_doc_count: int = 0


class TickerListResponse(BaseModel):
    tickers: list[TickerItem]
    total: int
