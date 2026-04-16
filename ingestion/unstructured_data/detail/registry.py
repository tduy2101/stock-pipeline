"""
Detail fetcher registry — dispatch URLs to the right domain-specific fetcher.

Usage::

    from ingestion.unstructured_data.detail import fetch_details

    articles_df = fetch_details(discoveries_df, cfg)
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from ..config import NewsIngestionConfig
from ..schemas import (
    ARTICLE_COLUMNS,
    compute_article_id,
    compute_content_hash,
    compact_text,
    empty_article_frame,
    looks_like_disclosure,
    strip_html,
)
from .base import DetailFetcher
from .cafef import CafeFDetailFetcher
from .vnexpress import VnExpressDetailFetcher

LOGGER = logging.getLogger(__name__)

# ── Built-in fetchers (order = priority) ────────────────────────────────────

_BUILTIN_FETCHERS: list[type[DetailFetcher]] = [
    VnExpressDetailFetcher,
    CafeFDetailFetcher,
]


def _build_fetchers(cfg: NewsIngestionConfig) -> list[DetailFetcher]:
    """Instantiate all built-in fetchers with settings from *cfg*."""
    return [
        cls(
            rate_limit_rpm=cfg.rate_limit_rpm,
            timeout_sec=cfg.detail_fetch_timeout_sec,
            retry_max=cfg.api_retry_max_attempts,
            retry_base_delay=cfg.api_retry_base_delay_sec,
        )
        for cls in _BUILTIN_FETCHERS
    ]


def _pick_fetcher(
    url: str, fetchers: list[DetailFetcher]
) -> DetailFetcher | None:
    for f in fetchers:
        if f.can_handle(url):
            return f
    return None


# ── Public API ──────────────────────────────────────────────────────────────

def fetch_details(
    discoveries_df: pd.DataFrame,
    cfg: NewsIngestionConfig,
) -> pd.DataFrame:
    """Fetch article detail pages for discoveries that have a known fetcher.

    Respects ``cfg.max_detail_fetch_per_run`` and deduplicates by
    ``canonical_url`` before fetching.

    Returns a DataFrame with :data:`ARTICLE_COLUMNS`.
    """
    if discoveries_df.empty or not cfg.enable_detail_fetch:
        return empty_article_frame()

    fetchers = _build_fetchers(cfg)
    if not fetchers:
        return empty_article_frame()

    deduped = discoveries_df.drop_duplicates(subset=["canonical_url"], keep="first")
    fetchable = deduped[deduped["canonical_url"].astype(str).str.startswith("http")]

    urls_to_fetch = fetchable.head(cfg.max_detail_fetch_per_run)
    total = len(urls_to_fetch)
    if total == 0:
        return empty_article_frame()

    LOGGER.info(
        "fetch_details: will fetch %d / %d unique URLs (max=%d)",
        total,
        len(fetchable),
        cfg.max_detail_fetch_per_run,
    )

    fetched_at = datetime.now(timezone.utc).isoformat()
    results: list[dict[str, Any]] = []
    success_count = 0
    fail_count = 0

    def _process_row(row: dict[str, Any]) -> dict[str, Any] | None:
        url = row.get("canonical_url", "")
        fetcher = _pick_fetcher(url, fetchers)
        if fetcher is None:
            return None
        detail = fetcher.fetch_one(row)
        body = detail.get("body_text", "")
        if not body:
            return None

        title = detail.get("title") or row.get("title", "")
        summary = detail.get("summary") or row.get("summary", "")
        is_disc = looks_like_disclosure(title)
        is_long = len(body) >= cfg.detail_min_body_length

        return {
            "article_id": compute_article_id(
                url, row.get("source", ""), detail.get("published_at") or row.get("published_at", "")
            ),
            "discovery_id": row.get("discovery_id", ""),
            "source": row.get("source", ""),
            "source_type": "html_detail",
            "content_type": (
                "disclosure" if is_disc else ("article" if is_long else "snippet")
            ),
            "ticker": "",
            "title": compact_text(title),
            "summary": compact_text(strip_html(summary)),
            "body_text": body,
            "url": row.get("url", ""),
            "canonical_url": url,
            "published_at": detail.get("published_at") or row.get("published_at", ""),
            "fetched_at": fetched_at,
            "language": row.get("language", "vi"),
            "author": compact_text(detail.get("author", "")),
            "tags": detail.get("tags", ""),
            "raw_ref": row.get("raw_ref", ""),
            "content_hash": compute_content_hash(body),
        }

    rows_dicts = urls_to_fetch.to_dict("records")

    max_workers = min(4, total)
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_process_row, row): row for row in rows_dicts}
        for future in as_completed(futures):
            try:
                result = future.result()
                if result and result.get("body_text"):
                    results.append(result)
                    success_count += 1
                else:
                    fail_count += 1
            except Exception as ex:
                fail_count += 1
                LOGGER.warning("fetch_details: thread error: %s", ex)

    LOGGER.info(
        "fetch_details: %d success, %d failed/empty out of %d attempted",
        success_count,
        fail_count,
        total,
    )

    if not results:
        return empty_article_frame()
    return pd.DataFrame(results, columns=ARTICLE_COLUMNS)
