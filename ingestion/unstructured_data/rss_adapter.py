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
    build_ticker_regex,
    compact_text,
    compute_article_id,
    dedupe_news,
    empty_news_frame,
    infer_ticker,
    normalize_url,
    parse_datetime_to_iso_utc,
    safe_json_dumps,
    strip_html,
)

LOGGER = logging.getLogger(__name__)


def _source_from_feed(url: str) -> str:
    host = (urlparse(url).hostname or "rss").lower().replace(".", "_")
    return f"rss_{host}"


def _label_source(label: str | None, url: str) -> str:
    if label:
        cleaned = compact_text(label).lower().replace(" ", "_")
        if cleaned:
            return f"{cleaned}_rss"
    return _source_from_feed(url)


def _entry_value(entry: object, *keys: str) -> object | None:
    for key in keys:
        val = getattr(entry, key, None)
        if val not in (None, ""):
            return val
        if isinstance(entry, dict):
            val = entry.get(key)
            if val not in (None, ""):
                return val
    return None


def _entry_published_at(entry: object) -> str | None:
    return parse_datetime_to_iso_utc(
        _entry_value(
            entry,
            "published_parsed",
            "updated_parsed",
            "created_parsed",
            "dc_date",
            "date",
            "published",
            "updated",
            "created",
            "pubdate",
            "issued",
            "modified",
        )
    )


def fetch_rss_news(cfg: NewsIngestionConfig, feed_specs: list[dict[str, str]]) -> pd.DataFrame:
    if not feed_specs:
        return empty_news_frame()
    fetched_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    rows: list[dict[str, str | None]] = []
    max_per = int(getattr(cfg, "rss_max_per_feed", 0) or cfg.max_articles_per_source)
    max_per = max(1, max_per)

    ticker_re = (
        build_ticker_regex(cfg.resolved_tickers()) if cfg.enable_ticker_match else None
    )
    session = requests.Session()
    session.headers.update(cfg.http_headers)

    for spec in feed_specs:
        feed_url = str(spec.get("url", "")).strip()
        if not feed_url:
            continue
        wait_for_rate_limit(cfg.rate_limit_rpm)

        def _get() -> str:
            r = session.get(feed_url, timeout=cfg.timeout_sec, headers=cfg.http_headers)
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

        source = _label_source(spec.get("label"), feed_url)
        entries_all = list(getattr(parsed, "entries", []) or [])
        LOGGER.info(
            "RSS feed %s: parsed_entries=%d (take<=%d)",
            feed_url,
            len(entries_all),
            max_per,
        )
        entries = entries_all[:max_per]
        for entry in entries:
            title = compact_text(getattr(entry, "title", ""))
            url = normalize_url(getattr(entry, "link", ""))
            if not title or not url:
                continue
            summary = strip_html(
                getattr(entry, "summary", "") or getattr(entry, "description", "")
            )
            content_value = ""
            content = getattr(entry, "content", None) or []
            if content:
                content_value = strip_html(content[0].get("value", ""))
            published_at = _entry_published_at(entry)
            ticker = infer_ticker([title, summary, content_value], ticker_re)
            article_id = compute_article_id(
                url=url,
                source=source,
                published_at=published_at or "",
                ticker=ticker or "",
                title=title,
            )
            rows.append(
                {
                    "article_id": article_id,
                    "source": source,
                    "ticker": ticker,
                    "title": title,
                    "summary": summary,
                    "body_text": content_value or "",
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
