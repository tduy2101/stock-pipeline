"""
vnstock adapter — **Discovery tier** + optional direct Article creation.

* **KBS**: produces discovery-only records (title + short head).
* **VCI**: if ``news_full_content`` is long enough, produces article-tier
  records directly (avoiding a second HTTP fetch).

Both tiers share the discovery ID so they can be joined later.
"""

from __future__ import annotations

import inspect
import logging
import time
from typing import Any

import pandas as pd
from vnstock import Company

from ingestion.common import call_with_retry, wait_for_rate_limit

from .config import NewsIngestionConfig
from .schemas import (
    ARTICLE_COLUMNS,
    DISCOVERY_COLUMNS,
    canonicalize_url,
    compact_text,
    compute_article_id,
    compute_content_hash,
    compute_discovery_id,
    empty_article_frame,
    empty_discovery_frame,
    looks_like_disclosure,
    na_safe_str,
    strip_html,
)

LOGGER = logging.getLogger(__name__)
KBS_NEWS_MAX_PAGE_SIZE = 20


# ── Internal helpers ────────────────────────────────────────────────────────

def _news_supports_paging(company: Any) -> bool:
    try:
        sig = inspect.signature(company.news)
        return "page" in sig.parameters and "page_size" in sig.parameters
    except (TypeError, ValueError):
        return False


def _is_kbs_news_shape(df: pd.DataFrame) -> bool:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return False
    cols = {str(c).strip().lower() for c in df.columns}
    return "head" in cols and "publish_time" in cols


def _looks_like_vnstock_api_error_df(df: pd.DataFrame) -> bool:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return False
    cols = {str(c).strip().lower() for c in df.columns}
    return ("st" in cols and "msg" in cols) or "error_code" in cols


def _parse_ts(val: Any) -> str | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, pd.Timestamp):
        if pd.isna(val):
            return None
        return val.isoformat()
    dt = pd.to_datetime(val, errors="coerce")
    if pd.isna(dt):
        return None
    try:
        return dt.isoformat()
    except Exception:
        return str(val)


# ── KBS normalizer → Discovery ─────────────────────────────────────────────

def _normalize_kbs_to_discovery(
    df: pd.DataFrame, *, ticker: str, data_source: str, fetched_at: str
) -> pd.DataFrame:
    """Map raw KBS DataFrame to DISCOVERY_COLUMNS."""
    if df.empty:
        return empty_discovery_frame()
    x = df.copy()
    x.columns = [str(c).strip().lower() for c in x.columns]

    title_col = "title" if "title" in x.columns else None
    head_col = "head" if "head" in x.columns else None
    url_col = "url" if "url" in x.columns else None
    pub_col = next(
        (c for c in ("publish_time", "published_at", "public_date") if c in x.columns),
        None,
    )

    source = f"vnstock_{data_source}"
    rows: list[dict[str, Any]] = []
    for _, row in x.iterrows():
        title = compact_text(row.get(title_col) if title_col else None)
        head = compact_text(row.get(head_col) if head_col else None)
        raw_url = na_safe_str(row.get(url_col) if url_col else None)
        curl = canonicalize_url(raw_url, base_url=None)
        pub = _parse_ts(row.get(pub_col) if pub_col else None)
        summary = strip_html(head) if head else ""

        rows.append(
            {
                "discovery_id": compute_discovery_id(curl, source, pub or "", title),
                "source": source,
                "source_type": "vnstock",
                "section": ticker.upper(),
                "title": title,
                "summary": summary,
                "url": raw_url,
                "canonical_url": curl,
                "published_at": pub,
                "fetched_at": fetched_at,
                "language": "vi",
                "raw_ref": curl or f"vnstock:{data_source}:{ticker}",
            }
        )
    return pd.DataFrame(rows, columns=DISCOVERY_COLUMNS)


# ── VCI normalizer → Discovery + optional Articles ─────────────────────────

def _normalize_vci_to_discovery_and_articles(
    df: pd.DataFrame,
    *,
    ticker: str,
    data_source: str,
    fetched_at: str,
    min_body_length: int = 200,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return ``(discovery_df, articles_df)`` from VCI data.

    If ``news_full_content`` is long enough the article is produced directly.
    """
    if df.empty:
        return empty_discovery_frame(), empty_article_frame()
    x = df.copy()
    x.columns = [str(c).strip().lower() for c in x.columns]

    title_c = "news_title" if "news_title" in x.columns else None
    short_c = "news_short_content" if "news_short_content" in x.columns else None
    full_c = "news_full_content" if "news_full_content" in x.columns else None
    link_c = "news_source_link" if "news_source_link" in x.columns else None
    pub_c = next(
        (c for c in ("public_date", "created_at", "updated_at") if c in x.columns),
        None,
    )

    source = f"vnstock_{data_source}"
    disc_rows: list[dict[str, Any]] = []
    art_rows: list[dict[str, Any]] = []

    for _, row in x.iterrows():
        title = compact_text(row.get(title_c) if title_c else None)
        short_raw = row.get(short_c) if short_c else None
        full_raw = row.get(full_c) if full_c else None
        summary = strip_html(short_raw) if short_raw else ""
        body = strip_html(full_raw) if full_raw else ""

        raw_url = na_safe_str(row.get(link_c) if link_c else None)
        curl = canonicalize_url(raw_url, base_url=None)
        pub = _parse_ts(row.get(pub_c) if pub_c else None)
        lang = compact_text(row.get("lang_code")) or "vi"

        did = compute_discovery_id(curl, source, pub or "", title)

        disc_rows.append(
            {
                "discovery_id": did,
                "source": source,
                "source_type": "vnstock",
                "section": ticker.upper(),
                "title": title,
                "summary": summary if summary else "",
                "url": raw_url,
                "canonical_url": curl,
                "published_at": pub,
                "fetched_at": fetched_at,
                "language": lang,
                "raw_ref": curl or f"vnstock:{data_source}:{ticker}",
            }
        )

        if body and len(body) >= min_body_length:
            is_disc = looks_like_disclosure(title)
            art_rows.append(
                {
                    "article_id": compute_article_id(curl, source, pub or ""),
                    "discovery_id": did,
                    "source": source,
                    "source_type": "vnstock",
                    "content_type": "disclosure" if is_disc else "article",
                    "ticker": ticker.upper(),
                    "title": title,
                    "summary": summary,
                    "body_text": body,
                    "url": raw_url,
                    "canonical_url": curl,
                    "published_at": pub,
                    "fetched_at": fetched_at,
                    "language": lang,
                    "author": "",
                    "tags": "",
                    "raw_ref": curl or f"vnstock:{data_source}:{ticker}",
                    "content_hash": compute_content_hash(body),
                }
            )

    disc_df = pd.DataFrame(disc_rows, columns=DISCOVERY_COLUMNS) if disc_rows else empty_discovery_frame()
    art_df = pd.DataFrame(art_rows, columns=ARTICLE_COLUMNS) if art_rows else empty_article_frame()
    return disc_df, art_df


# ── Public adapter class ───────────────────────────────────────────────────

class VnstockNewsAdapter:
    """Fetch company news via vnstock ``Company.news``.

    Returns:
        ``(discovery_df, articles_df)`` — articles are only non-empty when VCI
        provides full content.
    """

    def __init__(self, cfg: NewsIngestionConfig) -> None:
        self.cfg = cfg

    def fetch(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Return ``(discovery_df, articles_df)``."""
        from datetime import datetime, timezone

        tickers = [
            str(t).strip().upper()
            for t in self.cfg.tickers[: self.cfg.max_tickers_per_run]
            if str(t).strip()
        ]
        if not tickers:
            LOGGER.info("VnstockNewsAdapter: no tickers — skip.")
            return empty_discovery_frame(), empty_article_frame()

        fetched_at = datetime.now(timezone.utc).isoformat()
        disc_parts: list[pd.DataFrame] = []
        art_parts: list[pd.DataFrame] = []
        n = len(tickers)

        for i, symbol in enumerate(tickers):
            for src in self.cfg.resolved_vnstock_sources():
                try:
                    wait_for_rate_limit(self.cfg.rate_limit_rpm)
                    company = Company(source=src, symbol=symbol)
                    src_l = str(src).strip().lower()
                    raw_df = pd.DataFrame()

                    if src_l == "kbs":
                        raw_df = self._try_paginated(symbol, src)
                    if raw_df.empty and _news_supports_paging(company):
                        raw_df = self._try_paginated(symbol, src)
                    if raw_df.empty:
                        def _once() -> pd.DataFrame:
                            return Company(source=src, symbol=symbol).news()

                        raw = call_with_retry(
                            _once,
                            max_attempts=self.cfg.api_retry_max_attempts,
                            base_delay_sec=self.cfg.api_retry_base_delay_sec,
                            label=f"news {symbol}@{src}",
                        )
                        raw_df = raw if isinstance(raw, pd.DataFrame) else pd.DataFrame()

                    if raw_df.empty:
                        continue

                    if _is_kbs_news_shape(raw_df):
                        disc = _normalize_kbs_to_discovery(
                            raw_df, ticker=symbol, data_source=src, fetched_at=fetched_at
                        )
                        if not disc.empty:
                            disc_parts.append(disc)
                            LOGGER.info(
                                "VnstockNewsAdapter [KBS]: %s @%s -> %d discoveries",
                                symbol, src, len(disc),
                            )
                            break
                    else:
                        disc, arts = _normalize_vci_to_discovery_and_articles(
                            raw_df,
                            ticker=symbol,
                            data_source=src,
                            fetched_at=fetched_at,
                            min_body_length=self.cfg.detail_min_body_length,
                        )
                        if not disc.empty:
                            disc_parts.append(disc)
                        if not arts.empty:
                            art_parts.append(arts)
                        LOGGER.info(
                            "VnstockNewsAdapter [VCI]: %s @%s -> %d disc, %d articles",
                            symbol, src, len(disc), len(arts),
                        )
                        if not disc.empty:
                            break
                except Exception as ex:
                    LOGGER.warning("VnstockNewsAdapter %s (%s): %s", symbol, src, ex)

            if i < n - 1:
                time.sleep(self.cfg.inter_request_delay_sec)

        disc_df = pd.concat(disc_parts, ignore_index=True) if disc_parts else empty_discovery_frame()
        art_df = pd.concat(art_parts, ignore_index=True) if art_parts else empty_article_frame()
        return disc_df, art_df

    def _try_paginated(self, symbol: str, src: str) -> pd.DataFrame:
        try:
            return self._fetch_kbs_paginated(symbol, src)
        except Exception as ex:
            LOGGER.debug("paginated news %s@%s: %s", symbol, src, ex)
            return pd.DataFrame()

    def _fetch_kbs_paginated(self, symbol: str, src: str) -> pd.DataFrame:
        max_n = max(1, self.cfg.max_articles_per_source)
        page = 1
        chunks: list[pd.DataFrame] = []
        remaining = max_n
        max_pages = 500
        while remaining > 0 and page <= max_pages:
            page_size = min(KBS_NEWS_MAX_PAGE_SIZE, remaining)

            def _page() -> pd.DataFrame:
                wait_for_rate_limit(self.cfg.rate_limit_rpm)
                c = Company(source=src, symbol=symbol)
                return c.news(page=page, page_size=page_size, show_log=False)

            batch = call_with_retry(
                _page,
                max_attempts=self.cfg.api_retry_max_attempts,
                base_delay_sec=self.cfg.api_retry_base_delay_sec,
                label=f"news {symbol}@{src} p{page}",
            )
            if batch is None or not isinstance(batch, pd.DataFrame) or batch.empty:
                break
            if _looks_like_vnstock_api_error_df(batch):
                try:
                    msg = batch.iloc[0].get("msg", batch.to_string())
                except Exception:
                    msg = batch.to_string()
                LOGGER.warning(
                    "KBS news %s@%s page %s: API error frame — %s",
                    symbol, src, page, msg,
                )
                return pd.DataFrame()
            chunks.append(batch)
            got = len(batch)
            page += 1
            remaining -= got
        if not chunks:
            return pd.DataFrame()
        out = pd.concat(chunks, ignore_index=True)
        aid_col = next(
            (c for c in out.columns if str(c).strip().lower() == "article_id"),
            None,
        )
        if aid_col:
            out = out.drop_duplicates(subset=[aid_col], keep="first")
        return out
