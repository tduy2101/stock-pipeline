"""vnstock adapter for one-layer news ingestion."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from vnstock import Company

from ingestion.common import (
    call_with_retry,
    register_vnstock_api_key_from_env,
    wait_for_rate_limit,
)

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


def _pick(row: pd.Series, *keys: str) -> Any:
    for k in keys:
        if k in row and compact_text(row.get(k)) != "":
            return row.get(k)
    return None


def _normalize_vnstock_url(raw: Any) -> str:
    url = normalize_url(raw)
    if url:
        return url
    text = compact_text(raw)
    if not text:
        return ""
    if text.startswith("/"):
        return normalize_url(f"https://vietstock.vn{text}")
    if not text.startswith(("http://", "https://")):
        return normalize_url(f"https://vietstock.vn/{text}")
    return normalize_url(text)


def _coerce_news_payload_to_df(raw: Any) -> pd.DataFrame:
    if isinstance(raw, pd.DataFrame):
        return raw
    if isinstance(raw, dict):
        payload = raw.get("data") or raw.get("items") or raw.get("results") or raw
        if isinstance(payload, list):
            return pd.DataFrame(payload)
        if isinstance(payload, dict):
            return pd.DataFrame(payload.get("data") or [])
        return pd.DataFrame()
    if isinstance(raw, list):
        return pd.DataFrame(raw)
    return pd.DataFrame()


def fetch_vnstock_news(cfg: NewsIngestionConfig) -> pd.DataFrame:
    try:
        register_vnstock_api_key_from_env()
        tickers = cfg.resolved_tickers()
    except Exception as ex:
        LOGGER.warning(
            "vnstock news skipped due setup error: ex_type=%s ex=%s",
            type(ex).__name__,
            ex,
        )
        return empty_news_frame()

    if not tickers:
        LOGGER.warning("vnstock news skipped: resolved tickers is empty")
        return empty_news_frame()

    fetched_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    rows: list[dict[str, Any]] = []
    max_per = max(1, int(cfg.max_articles_per_source))
    # NOTE:
    # days_back is a downstream filter applied in news_ingestor.
    # Increasing days_back does NOT make vnstock upstream return more records.

    LOGGER.info("vnstock news: %d tickers, max_per=%d", len(tickers), max_per)

    for symbol in tickers:
        try:
            merged_raw_df = pd.DataFrame()
            source_frames: list[pd.DataFrame] = []
            source_take_stats: list[str] = []
            attempt_errors: list[tuple[str, Exception]] = []
            for src in ("kbs", "vci"):
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
                    raw_df = _coerce_news_payload_to_df(raw)
                    if raw_df.empty:
                        LOGGER.info("vnstock news %s@%s: empty response", symbol, src)
                        continue

                    raw_df = raw_df.copy()
                    raw_df.columns = [str(c).strip().lower() for c in raw_df.columns]
                    raw_df["_vnstock_source"] = src

                    take_df = raw_df.head(max_per).copy()
                    source_frames.append(take_df)
                    source_take_stats.append(f"{src}={len(take_df)}")
                    LOGGER.info(
                        "vnstock news %s@%s: %d rows (take<=%d)",
                        symbol,
                        src,
                        len(raw_df),
                        max_per,
                    )
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
                except Exception as ex:
                    LOGGER.warning(
                        "vnstock error symbol=%s src=%s ex_type=%s ex=%s",
                        symbol,
                        src,
                        type(ex).__name__,
                        ex,
                    )
                    attempt_errors.append((src, ex))

            if not source_frames and attempt_errors:
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

            if not source_frames:
                continue

            merged_raw_df = pd.concat(source_frames, ignore_index=True)
            LOGGER.info(
                "vnstock news %s: merged_rows=%d from_sources=%s",
                symbol,
                len(merged_raw_df),
                ", ".join(source_take_stats),
            )

            seen_article_ids: set[str] = set()
            added_rows = 0
            for _, row in merged_raw_df.iterrows():
                title = compact_text(_pick(row, "news_title", "title", "head"))
                url = _normalize_vnstock_url(_pick(row, "news_source_link", "url", "link"))
                if not title or not url:
                    continue
                summary = strip_html(_pick(row, "news_short_content", "summary", "description", "head"))
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
                if article_id in seen_article_ids:
                    continue
                seen_article_ids.add(article_id)
                added_rows += 1
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
            LOGGER.info("vnstock news %s: rows_after_merge=%d", symbol, added_rows)
        except Exception as ex:
            LOGGER.warning(
                "vnstock ticker skipped symbol=%s due unexpected error: ex_type=%s ex=%s",
                symbol,
                type(ex).__name__,
                ex,
            )
            continue

    if not rows:
        return empty_news_frame()
    return dedupe_news(pd.DataFrame(rows, columns=NEWS_COLUMNS))
