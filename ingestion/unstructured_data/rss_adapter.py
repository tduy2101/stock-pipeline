from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import pandas as pd

import requests

from ingestion.common import call_with_retry, wait_for_rate_limit

from .config import NewsIngestionConfig
from .news_schema import NEWS_COLUMNS, _compact_text, empty_news_frame, normalize_url

LOGGER = logging.getLogger(__name__)


def _feed_hostname(feed_url: str) -> str:
    try:
        h = urlparse(feed_url).hostname or "rss"
        return h.lower()
    except Exception:
        return "rss"


def _entry_published_iso(entry: Any) -> str | None:
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        try:
            t = entry.published_parsed
            dt = datetime(
                t.tm_year,
                t.tm_mon,
                t.tm_mday,
                t.tm_hour,
                t.tm_min,
                t.tm_sec,
                tzinfo=timezone.utc,
            )
            return dt.isoformat()
        except Exception:
            pass
    if hasattr(entry, "updated_parsed") and entry.updated_parsed:
        try:
            t = entry.updated_parsed
            dt = datetime(
                t.tm_year,
                t.tm_mon,
                t.tm_mday,
                t.tm_hour,
                t.tm_min,
                t.tm_sec,
                tzinfo=timezone.utc,
            )
            return dt.isoformat()
        except Exception:
            pass
    raw = getattr(entry, "published", None) or getattr(entry, "updated", None)
    if raw:
        parsed = pd.to_datetime(raw, errors="coerce")
        if pd.notna(parsed):
            try:
                return parsed.isoformat()
            except Exception:
                return str(raw)
    return None


def _entry_link(entry: Any) -> str:
    link = getattr(entry, "link", None) or ""
    if isinstance(link, list) and link:
        link = link[0]
    return str(link).strip()


def _entry_summary(entry: Any) -> str:
    if hasattr(entry, "summary"):
        return _compact_text(entry.summary)
    if hasattr(entry, "description"):
        return _compact_text(entry.description)
    return ""


class RssAdapter:
    """Parses RSS/Atom feeds via ``feedparser`` into the shared news schema."""

    def __init__(
        self,
        cfg: NewsIngestionConfig,
        *,
        feed_urls: list[str] | None = None,
    ) -> None:
        self.cfg = cfg
        self._feed_urls = feed_urls

    def fetch(self) -> pd.DataFrame:
        try:
            import feedparser
        except ImportError as ex:
            LOGGER.warning("RssAdapter: feedparser not installed (%s) — skip.", ex)
            return empty_news_frame()

        urls = list(self._feed_urls if self._feed_urls is not None else self.cfg.rss_feed_urls)
        if not urls:
            LOGGER.info("RssAdapter: no rss_feed_urls — skip.")
            return empty_news_frame()

        rows: list[dict[str, Any]] = []
        max_per = max(1, self.cfg.max_articles_per_source)
        fetched_at = datetime.now(timezone.utc).isoformat()

        for feed_url in urls:
            wait_for_rate_limit(self.cfg.rate_limit_rpm)
            host = _feed_hostname(feed_url)
            source = f"rss_{host}"

            def _fetch() -> str:
                wait_for_rate_limit(self.cfg.rate_limit_rpm)
                r = requests.get(
                    feed_url,
                    timeout=30,
                    headers={
                        "User-Agent": (
                            "Mozilla/5.0 (compatible; stock-pipeline-news/1.0; +https://github.com/)"
                        ),
                        "Accept": "application/rss+xml, application/xml, text/xml;q=0.9, */*;q=0.8",
                    },
                )
                r.raise_for_status()
                return r.text

            try:
                body = call_with_retry(
                    _fetch,
                    max_attempts=self.cfg.api_retry_max_attempts,
                    base_delay_sec=self.cfg.api_retry_base_delay_sec,
                    label=f"rss:{host}",
                )
                parsed = feedparser.parse(body)
            except Exception as ex:
                LOGGER.warning("RssAdapter: failed to fetch/parse %s: %s", feed_url, ex)
                continue
            if getattr(parsed, "bozo", False) and not getattr(parsed, "entries", None):
                LOGGER.warning(
                    "RssAdapter: feed may be ill-formed %s: %s",
                    feed_url,
                    getattr(parsed, "bozo_exception", ""),
                )
            entries = list(getattr(parsed, "entries", []) or [])[:max_per]
            for entry in entries:
                title = _compact_text(getattr(entry, "title", None))
                url = normalize_url(_entry_link(entry))
                summary = _entry_summary(entry)
                if not summary and title:
                    summary = title
                pub = _entry_published_iso(entry)
                raw_ref = f"{feed_url} | {url}" if url else feed_url
                rows.append(
                    {
                        "article_id": "",
                        "source": source,
                        "ticker": pd.NA,
                        "title": title,
                        "summary": summary,
                        "body_text": summary,
                        "url": url,
                        "published_at": pub,
                        "fetched_at": fetched_at,
                        "language": pd.NA,
                        "raw_ref": raw_ref,
                    }
                )

        if not rows:
            return empty_news_frame()
        return pd.DataFrame(rows, columns=NEWS_COLUMNS)
