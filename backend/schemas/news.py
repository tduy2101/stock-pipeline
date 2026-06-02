from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel


class NewsDailyRow(BaseModel):
    ticker: str
    published_date: date
    news_count: int | None
    avg_sentiment_score: float | None
    positive_count: int | None
    negative_count: int | None
    neutral_count: int | None
    dominant_sentiment: str | None


class NewsArticleRow(BaseModel):
    article_id: str
    ticker: str | None
    ticker_mentions: list[str] | None
    title: str
    summary: str | None
    body_text: str | None
    url: str | None
    source: str | None
    published_at: datetime | None
    published_date: date | None
    sentiment_score: float | None
    sentiment_label: str | None
    word_count: int | None
    language: str | None
