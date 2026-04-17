"""RSS adapter for one-layer news ingestion."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from urllib.parse import urlparse

import feedparser
import pandas as pd
import requests

from ingestion.common import call_with_retry, wait_for_rate_limit

from .config import NewsIngestionConfig
from .schema import (
    NEWS_COLUMNS,
    compact_text,
    compute_article_id,
    dedupe_news,
    empty_news_frame,
    normalize_url,
    parse_datetime_to_iso_utc,
    safe_json_dumps,
    strip_html,
)

LOGGER = logging.getLogger(__name__)


def _source_from_feed(url: str) -> str:
    host = (urlparse(url).hostname or "rss").lower().replace(".", "_")
    return f"rss_{host}"


def fetch_rss_news(cfg: NewsIngestionConfig, feed_urls: list[str]) -> pd.DataFrame:
    if not feed_urls:
        return empty_news_frame()
    fetched_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    rows: list[dict[str, str | None]] = []
    max_per = max(1, int(cfg.max_articles_per_source))

    for feed_url in feed_urls:
        wait_for_rate_limit(cfg.rate_limit_rpm)

        def _get() -> str:
            r = requests.get(feed_url, timeout=cfg.timeout_sec)
            r.raise_for_status()
            return r.text

        try:
            content = call_with_retry(
                _get,
                max_attempts=cfg.api_retry_max_attempts,
                base_delay_sec=cfg.api_retry_base_delay_sec,
                label=f"rss:{feed_url}",
            )
            parsed = feedparser.parse(content)
        except Exception as ex:
            LOGGER.warning("Failed RSS feed %s: %s", feed_url, ex)
            continue

        source = _source_from_feed(feed_url)
        entries = list(getattr(parsed, "entries", []) or [])[:max_per]
        for entry in entries:
            title = compact_text(getattr(entry, "title", ""))
            url = normalize_url(getattr(entry, "link", ""))
            if not title or not url:
                continue
            summary = strip_html(
                getattr(entry, "summary", "") or getattr(entry, "description", "")
            )
            published_at = parse_datetime_to_iso_utc(
                getattr(entry, "published", "") or getattr(entry, "updated", "")
            )
            article_id = compute_article_id(
                url=url,
                source=source,
                published_at=published_at or "",
                ticker="",
                title=title,
            )
            rows.append(
                {
                    "article_id": article_id,
                    "source": source,
                    "ticker": None,
                    "title": title,
                    "summary": summary,
                    "body_text": "",
                    "url": url,
                    "published_at": published_at,
                    "fetched_at": fetched_at,
                    "language": "vi",
                    "raw_ref": safe_json_dumps(dict(entry)),
                }
            )

    if not rows:
        return empty_news_frame()
    return dedupe_news(pd.DataFrame(rows, columns=NEWS_COLUMNS))
