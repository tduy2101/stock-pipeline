from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from backend.database import close_pool
from backend.routers import (
    board,
    bctc,
    companies,
    financials,
    health,
    indicators,
    market,
    news,
    prices,
    tickers,
)

app = FastAPI(
    title="Stock Pipeline API",
    description="Read-only API for the Gold layer of the Vietnam stock pipeline.",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(health.router)
app.include_router(tickers.router)
app.include_router(market.router)
app.include_router(companies.router)
app.include_router(prices.router)
app.include_router(indicators.router)
app.include_router(financials.router)
app.include_router(board.router)
app.include_router(news.router)
app.include_router(bctc.router)


@app.on_event("shutdown")
def shutdown_event() -> None:
    close_pool()
