from __future__ import annotations

from datetime import date

from pydantic import BaseModel


class PriceRow(BaseModel):
    ticker: str
    trading_date: date
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    volume: int | None
    value: float | None
    daily_return: float | None


class IndicatorRow(BaseModel):
    ticker: str
    trading_date: date
    close: float | None
    ma7: float | None
    ma20: float | None
    ma50: float | None
    rsi14: float | None
    macd_line: float | None
    macd_signal: float | None
    macd_hist: float | None
    bb_upper: float | None
    bb_middle: float | None
    bb_lower: float | None
    volume_ma20: float | None = None
    obv: float | None = None
    volatility_20d: float | None
