from __future__ import annotations

import json
from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from psycopg2.extensions import connection as PgConn

from backend.config import DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE
from backend.database import fetchall_as_dict
from backend.dependencies import get_db
from backend.schemas.common import PaginatedResponse
from backend.schemas.news import (
    NewsArticleRow,
    NewsSignalRow,
    NewsSignalSummary,
    TopArticle,
)

router = APIRouter(prefix="/news", tags=["news"])

_TICKER_NEWS_WHERE = "ticker = %s"


def _parse_top_articles(value: Any) -> list[TopArticle] | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = json.loads(value)
    if not value:
        return None
    return [TopArticle(**article) for article in value]


def _signal_row(row: dict) -> NewsSignalRow:
    payload = dict(row)
    payload["top_articles"] = _parse_top_articles(payload.get("top_articles"))
    return NewsSignalRow(**payload)


@router.get("/articles", response_model=PaginatedResponse[NewsArticleRow])
def list_all_news_articles(
    ticker: str | None = Query(None, description="Filter by ticker."),
    q: str | None = Query(None, description="Search in title, summary, or body."),
    sentiment: str | None = Query(None, description="positive, neutral, or negative."),
    relevance: str | None = Query(None, description="title, summary, or body."),
    from_date: date | None = Query(None, alias="from"),
    to_date: date | None = Query(None, alias="to"),
    page: int = Query(1, ge=1),
    page_size: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    conn: PgConn = Depends(get_db),
):
    ticker_filter = ticker.upper() if ticker else None
    q_filter = f"%{q.strip()}%" if q and q.strip() else None
    sentiment_filter = sentiment.lower() if sentiment else None
    relevance_filter = relevance.lower() if relevance else None
    offset = (page - 1) * page_size

    params = (
        ticker_filter,
        ticker_filter,
        q_filter,
        q_filter,
        q_filter,
        q_filter,
        sentiment_filter,
        sentiment_filter,
        relevance_filter,
        relevance_filter,
        from_date,
        from_date,
        to_date,
        to_date,
    )

    where_clause = """
      (%s IS NULL OR ticker = %s)
      AND (%s IS NULL OR title ILIKE %s OR summary ILIKE %s OR body_text ILIKE %s)
      AND (%s IS NULL OR sentiment_label = %s)
      AND (%s IS NULL OR ticker_relevance = %s)
      AND (%s IS NULL OR published_date >= %s)
      AND (%s IS NULL OR published_date <= %s)
    """

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM gold.fact_news_article
            WHERE {where_clause}
            """,
            params,
        )
        total = cur.fetchone()[0]

        cur.execute(
            f"""
            SELECT
                article_id,
                ticker,
                ticker_mentions,
                title,
                summary,
                body_text,
                url,
                source,
                published_at,
                published_date,
                sentiment_score,
                sentiment_label,
                word_count,
                language,
                ticker_relevance,
                source_tier
            FROM gold.fact_news_article
            WHERE {where_clause}
            ORDER BY published_at DESC NULLS LAST,
                     published_date DESC NULLS LAST,
                     article_id,
                     ticker
            LIMIT %s OFFSET %s
            """,
            (*params, page_size, offset),
        )
        rows = fetchall_as_dict(cur)

    return PaginatedResponse(
        data=[NewsArticleRow(**row) for row in rows],
        total=total,
        page=page,
        page_size=page_size,
        has_more=(offset + len(rows)) < total,
    )


@router.get("/market", response_model=list[NewsArticleRow])
def get_market_news(
    page_size: int = Query(10, ge=1, le=MAX_PAGE_SIZE),
    conn: PgConn = Depends(get_db),
):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                article_id,
                ticker,
                ticker_mentions,
                title,
                summary,
                body_text,
                url,
                source,
                published_at,
                published_date,
                sentiment_score,
                sentiment_label,
                word_count,
                language,
                ticker_relevance,
                source_tier
            FROM gold.fact_news_article
            WHERE title IS NOT NULL
            ORDER BY published_at DESC NULLS LAST,
                     published_date DESC NULLS LAST,
                     article_id,
                     ticker
            LIMIT %s
            """,
            (page_size,),
        )
        rows = fetchall_as_dict(cur)

    return [NewsArticleRow(**row) for row in rows]


@router.get("/{symbol}/articles", response_model=PaginatedResponse[NewsArticleRow])
def get_news_articles(
    symbol: str,
    relevance: str | None = Query(None, description="title, summary, or body."),
    from_date: date | None = Query(None, alias="from"),
    to_date: date | None = Query(None, alias="to"),
    page: int = Query(1, ge=1),
    page_size: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    conn: PgConn = Depends(get_db),
):
    ticker = symbol.upper()
    relevance_filter = relevance.lower() if relevance else None
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT COUNT(*)
            FROM gold.fact_news_article
            WHERE {_TICKER_NEWS_WHERE}
              AND (%s IS NULL OR ticker_relevance = %s)
              AND (%s IS NULL OR published_date >= %s)
              AND (%s IS NULL OR published_date <= %s)
            """,
            (
                ticker,
                relevance_filter,
                relevance_filter,
                from_date,
                from_date,
                to_date,
                to_date,
            ),
        )
        total = cur.fetchone()[0]

        offset = (page - 1) * page_size
        cur.execute(
            f"""
            SELECT
                article_id,
                ticker,
                ticker_mentions,
                title,
                summary,
                body_text,
                url,
                source,
                published_at,
                published_date,
                sentiment_score,
                sentiment_label,
                word_count,
                language,
                ticker_relevance,
                source_tier
            FROM gold.fact_news_article
            WHERE {_TICKER_NEWS_WHERE}
              AND (%s IS NULL OR ticker_relevance = %s)
              AND (%s IS NULL OR published_date >= %s)
              AND (%s IS NULL OR published_date <= %s)
            ORDER BY published_at DESC NULLS LAST,
                     published_date DESC NULLS LAST,
                     article_id
            LIMIT %s OFFSET %s
            """,
            (
                ticker,
                relevance_filter,
                relevance_filter,
                from_date,
                from_date,
                to_date,
                to_date,
                page_size,
                offset,
            ),
        )
        rows = fetchall_as_dict(cur)

    return PaginatedResponse(
        data=[NewsArticleRow(**row) for row in rows],
        total=total,
        page=page,
        page_size=page_size,
        has_more=(offset + len(rows)) < total,
    )


@router.get("/{symbol}/signal", response_model=NewsSignalSummary)
def get_news_signal_summary(symbol: str, conn: PgConn = Depends(get_db)):
    ticker = symbol.upper()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                ticker,
                trading_date as latest_date,
                news_signal,
                weighted_sentiment,
                news_count,
                top_articles
            FROM gold.mart_stock_news_signal
            WHERE ticker = %s
            ORDER BY trading_date DESC
            LIMIT 1
            """,
            (ticker,),
        )
        row = cur.fetchone()
        if row is None:
            raise HTTPException(404, f"Khong co tin cho ma: {symbol}")
        cols = [desc[0] for desc in cur.description]
        payload = dict(zip(cols, row, strict=True))

    payload["top_articles"] = _parse_top_articles(payload.get("top_articles"))
    if payload["top_articles"] is not None:
        payload["top_articles"] = payload["top_articles"][:3]
    return NewsSignalSummary(**payload)


@router.get("/{symbol}", response_model=PaginatedResponse[NewsSignalRow])
def get_news(
    symbol: str,
    from_date: date | None = Query(None, alias="from"),
    to_date: date | None = Query(None, alias="to"),
    page: int = Query(1, ge=1),
    page_size: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
    conn: PgConn = Depends(get_db),
):
    ticker = symbol.upper()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM gold.mart_stock_news_signal
            WHERE ticker = %s
              AND (%s IS NULL OR trading_date >= %s)
              AND (%s IS NULL OR trading_date <= %s)
            """,
            (ticker, from_date, from_date, to_date, to_date),
        )
        total = cur.fetchone()[0]

        offset = (page - 1) * page_size
        cur.execute(
            """
            SELECT
                ticker,
                trading_date,
                news_count,
                positive_count,
                negative_count,
                neutral_count,
                avg_sentiment_score,
                weighted_sentiment,
                dominant_sentiment,
                news_signal,
                top_articles
            FROM gold.mart_stock_news_signal
            WHERE ticker = %s
              AND (%s IS NULL OR trading_date >= %s)
              AND (%s IS NULL OR trading_date <= %s)
            ORDER BY trading_date DESC
            LIMIT %s OFFSET %s
            """,
            (ticker, from_date, from_date, to_date, to_date, page_size, offset),
        )
        rows = fetchall_as_dict(cur)

    return PaginatedResponse(
        data=[_signal_row(row) for row in rows],
        total=total,
        page=page,
        page_size=page_size,
        has_more=(offset + len(rows)) < total,
    )
