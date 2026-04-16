from __future__ import annotations

import inspect
import logging
import time
from typing import Any

import pandas as pd
from vnstock import Company

from ingestion.common import call_with_retry, wait_for_rate_limit

from .config import NewsIngestionConfig
from .news_schema import (
    NEWS_COLUMNS,
    _compact_text,
    _parse_ts,
    empty_news_frame,
    normalize_url,
    strip_html_to_text,
)

LOGGER = logging.getLogger(__name__)


def _news_supports_paging(company: Any) -> bool:
    try:
        sig = inspect.signature(company.news)
        params = sig.parameters
        return "page" in params and "page_size" in params
    except (TypeError, ValueError):
        return False


def _is_kbs_news_shape(df: pd.DataFrame) -> bool:
    """KBS-style ``Company.news`` has ``head`` + ``publish_time``; VCI uses ``news_title`` etc."""
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return False
    cols = {str(c).strip().lower() for c in df.columns}
    return "head" in cols and "publish_time" in cols


def _looks_like_vnstock_api_error_df(df: pd.DataFrame) -> bool:
    """KBS trả khung lỗi (vd. ``page_size`` > 20) với cột ``st`` / ``msg`` thay vì tin."""
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return False
    cols = {str(c).strip().lower() for c in df.columns}
    if "st" in cols and "msg" in cols:
        return True
    if "error_code" in cols:
        return True
    return False


# KBS ``Company.news``: tham số ``page_size`` tối đa 20 (lỗi "s must not be greater than 20").
KBS_NEWS_MAX_PAGE_SIZE = 20


def _normalize_kbs_vnstock_df(
    df: pd.DataFrame, *, ticker: str, data_source: str
) -> pd.DataFrame:
    if df.empty:
        return empty_news_frame()
    x = df.copy()
    x.columns = [str(c).strip().lower() for c in x.columns]
    title_col = "title" if "title" in x.columns else None
    head_col = "head" if "head" in x.columns else None
    url_col = "url" if "url" in x.columns else None
    upstream_id_col = "article_id" if "article_id" in x.columns else None
    pub_col = next(
        (c for c in ("publish_time", "published_at", "public_date") if c in x.columns),
        None,
    )
    rows: list[dict[str, Any]] = []
    for _, row in x.iterrows():
        title = _compact_text(row.get(title_col) if title_col else None)
        head = _compact_text(row.get(head_col) if head_col else None)
        url = normalize_url(row.get(url_col) if url_col else None)
        pub = _parse_ts(row.get(pub_col) if pub_col else None)
        summary = head if head else title
        body = summary
        aid = ""
        if upstream_id_col:
            raw_aid = row.get(upstream_id_col)
            if raw_aid is not None and not (isinstance(raw_aid, float) and pd.isna(raw_aid)):
                aid = _compact_text(str(raw_aid))
        rows.append(
            {
                "article_id": aid,
                "source": f"vnstock_{data_source}",
                "ticker": ticker.upper().strip(),
                "title": title,
                "summary": summary,
                "body_text": body,
                "url": url,
                "published_at": pub,
                "fetched_at": "",
                "language": "vi",
                "raw_ref": url if url else pd.NA,
            }
        )
    return pd.DataFrame(rows, columns=NEWS_COLUMNS)


def _normalize_vci_vnstock_df(
    df: pd.DataFrame, *, ticker: str, data_source: str
) -> pd.DataFrame:
    if df.empty:
        return empty_news_frame()
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
    rows: list[dict[str, Any]] = []
    for _, row in x.iterrows():
        title = _compact_text(row.get(title_c) if title_c else None)
        summary = strip_html_to_text(row.get(short_c) if short_c else None)
        if not summary:
            summary = _compact_text(row.get(short_c) if short_c else None)
        body = strip_html_to_text(row.get(full_c) if full_c else None)
        if not body:
            body = summary
        if not summary and title:
            summary = title
        if not body:
            body = summary
        url = normalize_url(row.get(link_c) if link_c else None)
        pub = _parse_ts(row.get(pub_c) if pub_c else None)
        lang = _compact_text(row.get("lang_code")) or "vi"
        rows.append(
            {
                "article_id": "",
                "source": f"vnstock_{data_source}",
                "ticker": ticker.upper().strip(),
                "title": title,
                "summary": summary,
                "body_text": body,
                "url": url,
                "published_at": pub,
                "fetched_at": "",
                "language": lang,
                "raw_ref": url if url else pd.NA,
            }
        )
    return pd.DataFrame(rows, columns=NEWS_COLUMNS)


class VnstockNewsAdapter:
    """Fetches company news via vnstock ``Company.news`` and maps to the shared schema."""

    def __init__(self, cfg: NewsIngestionConfig) -> None:
        self.cfg = cfg

    def fetch(self) -> pd.DataFrame:
        tickers = [
            str(t).strip().upper()
            for t in self.cfg.tickers[: self.cfg.max_tickers_per_run]
            if str(t).strip()
        ]
        if not tickers:
            LOGGER.info("VnstockNewsAdapter: no tickers — skip.")
            return empty_news_frame()

        parts: list[pd.DataFrame] = []
        n = len(tickers)
        for i, symbol in enumerate(tickers):
            for src in self.cfg.resolved_vnstock_sources():
                try:
                    wait_for_rate_limit(self.cfg.rate_limit_rpm)

                    def _make_company() -> Any:
                        return Company(source=src, symbol=symbol)

                    company = _make_company()
                    src_l = str(src).strip().lower()
                    raw_df = pd.DataFrame()

                    def _try_paginated() -> pd.DataFrame:
                        try:
                            return self._fetch_kbs_paginated(symbol, src)
                        except Exception as ex:
                            LOGGER.debug(
                                "paginated news %s@%s: %s", symbol, src, ex
                            )
                            return pd.DataFrame()

                    # KBS: luôn thử phân trang trước (nhiều tin trong cửa sổ thời gian).
                    if src_l == "kbs":
                        raw_df = _try_paginated()
                    if raw_df.empty and _news_supports_paging(company):
                        raw_df = _try_paginated()
                    if raw_df.empty:

                        def _once() -> pd.DataFrame:
                            return Company(source=src, symbol=symbol).news()

                        raw = call_with_retry(
                            _once,
                            max_attempts=self.cfg.api_retry_max_attempts,
                            base_delay_sec=self.cfg.api_retry_base_delay_sec,
                            label=f"news {symbol}@{src}",
                        )
                        raw_df = (
                            raw if isinstance(raw, pd.DataFrame) else pd.DataFrame()
                        )

                    if _is_kbs_news_shape(raw_df):
                        norm = _normalize_kbs_vnstock_df(
                            raw_df, ticker=symbol, data_source=src
                        )
                    else:
                        norm = _normalize_vci_vnstock_df(
                            raw_df, ticker=symbol, data_source=src
                        )
                    if not norm.empty:
                        parts.append(norm)
                        LOGGER.info(
                            "VnstockNewsAdapter: %s @%s -> %s rows",
                            symbol,
                            src,
                            len(norm),
                        )
                        break
                except Exception as ex:
                    LOGGER.warning("VnstockNewsAdapter %s (%s): %s", symbol, src, ex)
            if i < n - 1:
                time.sleep(self.cfg.inter_request_delay_sec)

        if not parts:
            return empty_news_frame()
        return pd.concat(parts, ignore_index=True)

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
                    symbol,
                    src,
                    page,
                    msg,
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
