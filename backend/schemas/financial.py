from __future__ import annotations

from pydantic import BaseModel


class FinancialRatioRow(BaseModel):
    ticker: str
    period: str
    period_type: str | None
    year: int | None
    quarter: int | None
    item_code: str
    item_name: str | None
    value: float | None
