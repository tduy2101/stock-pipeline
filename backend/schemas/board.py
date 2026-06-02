from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel


class PriceBoardRow(BaseModel):
    symbol: str
    trading_date: date
    exchange: str | None = None
    ceiling_price: float | None = None
    floor_price: float | None = None
    reference_price: float | None = None
    close_price: float | None = None
    open_price: float | None = None
    high_price: float | None = None
    low_price: float | None = None
    average_price: float | None = None
    percent_change: float | None = None
    price_change: float | None = None
    volume_accumulated: float | None = None
    total_value: float | None = None
    bid_price_1: float | None = None
    bid_vol_1: float | None = None
    bid_price_2: float | None = None
    bid_vol_2: float | None = None
    bid_price_3: float | None = None
    bid_vol_3: float | None = None
    ask_price_1: float | None = None
    ask_vol_1: float | None = None
    ask_price_2: float | None = None
    ask_vol_2: float | None = None
    ask_price_3: float | None = None
    ask_vol_3: float | None = None
    foreign_buy_volume: float | None = None
    foreign_sell_volume: float | None = None
    foreign_net_volume: float | None = None
    foreign_room: float | None = None
    spread_pct: float | None = None
    snapshot_at: datetime | None = None


class ForeignFlowRow(BaseModel):
    trading_date: date
    foreign_buy_volume: float | None = None
    foreign_sell_volume: float | None = None
    foreign_net_volume: float | None = None
    foreign_room: float | None = None
