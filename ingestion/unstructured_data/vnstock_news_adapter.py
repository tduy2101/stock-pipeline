"""vnstock adapter for one-layer news ingestion."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from vnstock import Company

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


def _load_tickers(cfg: NewsIngestionConfig) -> list[str]:
    if not cfg.use_listing_tickers:
        out = [compact_text(t).upper() for t in cfg.tickers if compact_text(t)]
        return out[: cfg.max_tickers_per_run]
    listing = cfg.resolved_listing_parquet()
    if not listing.is_file():
        LOGGER.warning("Listing parquet not found: %s", listing)
        return [compact_text(t).upper() for t in cfg.tickers if compact_text(t)][: cfg.max_tickers_per_run]
    df = pd.read_parquet(listing)
    if "symbol" not in df.columns:
        LOGGER.warning("Listing parquet missing symbol column")
        return [compact_text(t).upper() for t in cfg.tickers if compact_text(t)][: cfg.max_tickers_per_run]
    if cfg.listing_exchange_filter and "exchange" in df.columns:
        allow = {compact_text(x).upper() for x in cfg.listing_exchange_filter if compact_text(x)}
        if allow:
            ex = df["exchange"].astype(str).str.upper().str.strip()
            df = df[ex.isin(allow)]
    symbols = (
        df["symbol"]
        .astype(str)
        .str.upper()
        .str.strip()
        .dropna()
        .unique()
        .tolist()
    )
    out = [s for s in symbols if s and s not in {"NAN", "NONE", "<NA>"}]
    return out[: cfg.max_tickers_per_run]


def _pick(row: pd.Series, *keys: str) -> Any:
    for k in keys:
        if k in row and compact_text(row.get(k)) != "":
            return row.get(k)
    return None


def fetch_vnstock_news(cfg: NewsIngestionConfig) -> pd.DataFrame:
    tickers = _load_tickers(cfg)
    if not tickers:
        return empty_news_frame()

    fetched_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    rows: list[dict[str, Any]] = []
    max_per = max(1, int(cfg.max_articles_per_source))

    for symbol in tickers:
        raw_df = pd.DataFrame()
        attempt_errors: list[tuple[str, Exception]] = []
        for src in ("KBS", "VCI"):
            wait_for_rate_limit(cfg.rate_limit_rpm)

            def _call() -> pd.DataFrame:
                company = Company(symbol=symbol, source=src)
                return company.news()

            try:
                raw = call_with_retry(
                    _call,
                    max_attempts=cfg.api_retry_max_attempts,
                    base_delay_sec=cfg.api_retry_base_delay_sec,
                    label=f"vnstock:{symbol}@{src.lower()}",
                )
                raw_df = raw if isinstance(raw, pd.DataFrame) else pd.DataFrame()
                if not raw_df.empty:
                    break
            except KeyError as ex:
                # Non-critical: some tickers/source responses do not include "data".
                LOGGER.info(
                    "vnstock no data for symbol=%s src=%s ex_type=%s ex=%s",
                    symbol,
                    src,
                    type(ex).__name__,
                    ex,
                )
                attempt_errors.append((src, ex))
                raw_df = pd.DataFrame()
            except Exception as ex:
                attempt_errors.append((src, ex))
                raw_df = pd.DataFrame()

        if raw_df.empty and attempt_errors:
            details = "; ".join(
                f"src={src} ex_type={type(err).__name__} ex={err}"
                for src, err in attempt_errors
            )
            LOGGER.warning(
                "vnstock fetch skipped symbol=%s after all sources failed: %s",
                symbol,
                details,
            )
            continue

        if raw_df.empty:
            continue

        raw_df.columns = [str(c).strip().lower() for c in raw_df.columns]
        for _, row in raw_df.head(max_per).iterrows():
            title = compact_text(_pick(row, "news_title", "title", "head"))
            url = normalize_url(_pick(row, "news_source_link", "url", "link"))
            if not title or not url:
                continue
            summary = strip_html(_pick(row, "news_short_content", "summary", "description"))
            body_text = strip_html(_pick(row, "news_full_content", "content", "body"))
            published_at = parse_datetime_to_iso_utc(_pick(row, "public_date", "publish_time", "published_at", "created_at"))
            source = "vnstock"
            ticker = symbol
            article_id = compute_article_id(
                url=url,
                source=source,
                published_at=published_at or "",
                ticker=ticker,
                title=title,
            )
            rows.append(
                {
                    "article_id": article_id,
                    "source": source,
                    "ticker": ticker,
                    "title": title,
                    "summary": summary,
                    "body_text": body_text or "",
                    "url": url,
                    "published_at": published_at,
                    "fetched_at": fetched_at,
                    "language": "vi",
                    "raw_ref": safe_json_dumps(row.to_dict()),
                }
            )

    if not rows:
        return empty_news_frame()
    return dedupe_news(pd.DataFrame(rows, columns=NEWS_COLUMNS))
