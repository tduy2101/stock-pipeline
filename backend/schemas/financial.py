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


class FinancialSummaryRow(BaseModel):
    ticker: str
    period: str
    period_type: str | None
    year: int | None
    quarter: int | None
    pe_ratio: float | None = None
    pb_ratio: float | None = None
    ps_ratio: float | None = None
    ev_ebit: float | None = None
    ev_ebitda: float | None = None
    eps: float | None = None
    roe: float | None = None
    roe_trailing: float | None = None
    roa: float | None = None
    roa_trailing: float | None = None
    roce: float | None = None
    gross_profit_margin: float | None = None
    net_profit_margin: float | None = None
    ebit_margin: float | None = None
    ebitda_margin: float | None = None
    current_ratio: float | None = None
    quick_ratio: float | None = None
    cash_ratio: float | None = None
    debt_to_equity: float | None = None
    debt_to_assets: float | None = None
    liabilities_to_equity: float | None = None
    liabilities_to_assets: float | None = None
    revenue_growth: float | None = None
    gross_profit_growth: float | None = None
    profit_growth: float | None = None
    dividend_yield: float | None = None
    dividend_per_share: float | None = None
    book_value_per_share: float | None = None
    cash_flow_per_share: float | None = None
    beta: float | None = None
