from __future__ import annotations

from datetime import date
from typing import Any

from pydantic import BaseModel


class MarketOverviewResponse(BaseModel):
    trading_date: date
    vnindex_close: float | None
    vnindex_return: float | None
    vn30_close: float | None
    vn30_return: float | None
    hnx_close: float | None
    hnx_return: float | None
    total_volume: int | None
    total_value: float | None
    advances: int | None
    declines: int | None
    unchanged: int | None
    top_gainers: Any | None
    top_losers: Any | None
    universe_size: int | None = None
