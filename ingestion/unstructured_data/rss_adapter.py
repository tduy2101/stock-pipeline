"""
RSS / Atom feed adapter — **Discovery tier only**.

Parses feeds via ``feedparser``, strips HTML from summaries, and outputs
clean :data:`DISCOVERY_COLUMNS` records.  ``body_text`` is left to the
detail-fetch tier.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

import pandas as pd
import requests

from ingestion.common import call_with_retry, wait_for_rate_limit

from .config import NewsIngestionConfig
from .schemas import (
    DISCOVERY_COLUMNS,
    canonicalize_url,
    compact_text,
    compute_discovery_id,
    empty_discovery_frame,
    strip_html,
)

LOGGER = logging.getLogger(__name__)


def _feed_hostname(feed_url: str) -> str:
    try:
        h = urlparse(feed_url).hostname or "rss"
        return h.lower()
    except Exception:
        return "rss"


def _section_from_feed_url(feed_url: str) -> str:
    """Best-effort section extraction, e.g. ``kinh-doanh`` from the path."""
    try:
        path = urlparse(feed_url).path or ""
        parts = [p for p in path.strip("/").split("/") if p and p != "rss"]
        if parts:
            segment = parts[-1]
            if segment.endswith(".rss"):
                segment = segment[:-4]
            return segment
        return ""
    except Exception:
        return ""


def _entry_published_iso(entry: Any) -> str | None:
    for attr in ("published_parsed", "updated_parsed"):
        tp = getattr(entry, attr, None)
        if tp:
            try:
                dt = datetime(
                    tp.tm_year, tp.tm_mon, tp.tm_mday,
                    tp.tm_hour, tp.tm_min, tp.tm_sec,
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


class RssAdapter:
    """Parse RSS/Atom feeds and emit **DISCOVERY** records.

    Summaries are cleaned with :func:`strip_html` to remove ``<img>``,
    ``<a>`` and other markup commonly found in RSS descriptions.
    """

    def __init__(
        self,
        cfg: NewsIngestionConfig,
        *,
        feed_urls: list[str] | None = None,
    ) -> None:
        self.cfg = cfg
        self._feed_urls = feed_urls

    def fetch(self) -> pd.DataFrame:
        """Return a DataFrame with :data:`DISCOVERY_COLUMNS`."""
        try:
            import feedparser
        except ImportError as ex:
            LOGGER.warning("RssAdapter: feedparser not installed (%s) — skip.", ex)
            return empty_discovery_frame()

        urls = list(
            self._feed_urls if self._feed_urls is not None else self.cfg.rss_feed_urls
        )
        if not urls:
            LOGGER.info("RssAdapter: no rss_feed_urls — skip.")
            return empty_discovery_frame()

        rows: list[dict[str, Any]] = []
        max_per = max(1, self.cfg.max_articles_per_source)
        fetched_at = datetime.now(timezone.utc).isoformat()

        for feed_url in urls:
            wait_for_rate_limit(self.cfg.rate_limit_rpm)
            host = _feed_hostname(feed_url)
            source = host
            section = _section_from_feed_url(feed_url)

            def _fetch_feed() -> str:
                wait_for_rate_limit(self.cfg.rate_limit_rpm)
                r = requests.get(
                    feed_url,
                    timeout=30,
                    headers={
                        "User-Agent": (
                            "Mozilla/5.0 (compatible; stock-pipeline-news/1.0;"
                            " +https://github.com/)"
                        ),
                        "Accept": (
                            "application/rss+xml, application/xml, text/xml;q=0.9,"
                            " */*;q=0.8"
                        ),
                    },
                )
                r.raise_for_status()
                return r.text

            try:
                body = call_with_retry(
                    _fetch_feed,
                    max_attempts=self.cfg.api_retry_max_attempts,
                    base_delay_sec=self.cfg.api_retry_base_delay_sec,
                    label=f"rss:{host}",
                )
                parsed = feedparser.parse(body)
            except Exception as ex:
                LOGGER.warning(
                    "RssAdapter: failed to fetch/parse %s: %s", feed_url, ex
                )
                continue

            if getattr(parsed, "bozo", False) and not getattr(
                parsed, "entries", None
            ):
                LOGGER.warning(
                    "RssAdapter: feed may be ill-formed %s: %s",
                    feed_url,
                    getattr(parsed, "bozo_exception", ""),
                )

            entries = list(getattr(parsed, "entries", []) or [])[:max_per]
            for entry in entries:
                title = compact_text(getattr(entry, "title", None))
                raw_link = _entry_link(entry)
                curl = canonicalize_url(raw_link, base_url=None)
                raw_summary = getattr(entry, "summary", None) or getattr(
                    entry, "description", None
                )
                summary = strip_html(raw_summary) if raw_summary else ""

                pub = _entry_published_iso(entry)
                raw_ref = f"{feed_url} | {curl}" if curl else feed_url

                rows.append(
                    {
                        "discovery_id": compute_discovery_id(
                            curl, source, pub or "", title
                        ),
                        "source": source,
                        "source_type": "rss",
                        "section": section,
                        "title": title,
                        "summary": summary,
                        "url": raw_link,
                        "canonical_url": curl,
                        "published_at": pub,
                        "fetched_at": fetched_at,
                        "language": "vi",
                        "raw_ref": raw_ref,
                    }
                )

            LOGGER.info(
                "RssAdapter [%s/%s]: collected %d entries",
                host,
                section or "*",
                len(entries),
            )

        if not rows:
            return empty_discovery_frame()
        return pd.DataFrame(rows, columns=DISCOVERY_COLUMNS)
