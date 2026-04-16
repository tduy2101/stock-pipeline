from __future__ import annotations

import logging
from datetime import date
from typing import Any

import pandas as pd
from bs4 import BeautifulSoup
from dateutil.relativedelta import relativedelta
from vnstock import Company

from ingestion.common import call_with_retry, wait_for_rate_limit
from ingestion.unstructured_data.news_schema import normalize_url

from .bctc_keywords import is_bctc_candidate
from .config import BctcPdfConfig

LOGGER = logging.getLogger(__name__)


def _fetch_company_news_paginated(symbol: str, src_l: str, cfg: BctcPdfConfig) -> pd.DataFrame:
    """Gọi ``Company.news`` nhiều trang (VCI thường ~10 bài/trang) để tăng cơ hội thấy link PDF BCTC."""
    max_pages = max(1, int(cfg.discovery_max_news_pages))
    chunks: list[pd.DataFrame] = []
    for page in range(1, max_pages + 1):
        wait_for_rate_limit(cfg.rate_limit_rpm)

        cur_page = page

        def _news() -> pd.DataFrame:
            c = Company(source=src_l, symbol=symbol)
            try:
                return c.news(page=cur_page, page_size=50, show_log=False)
            except TypeError:
                return c.news()

        df = call_with_retry(
            _news,
            max_attempts=cfg.api_retry_max_attempts,
            base_delay_sec=cfg.api_retry_base_delay_sec,
            label=f"bctc_news {symbol}@{src_l} p{page}",
        )
        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            break
        chunks.append(df)
        if len(df) < 10:
            break
    if not chunks:
        return pd.DataFrame()
    out = pd.concat(chunks, ignore_index=True)
    cmap = _column_map(out)
    dedup_col = next((cmap[k] for k in ("news_id", "id") if k in cmap), None)
    if dedup_col:
        out = out.drop_duplicates(subset=[dedup_col], keep="first")
    return out


def _parse_ts_cell(val: Any) -> pd.Timestamp | None:
    """VCI hay trả ``public_date`` là epoch **milliseconds**; ``pd.to_datetime(int)`` mặc định sai (1970)."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, pd.Timestamp):
        return val if pd.notna(val) else None
    if isinstance(val, (int, float)) and not pd.isna(val):
        x = abs(float(val))
        if x > 1e15:
            dt = pd.to_datetime(val, unit="ns", errors="coerce")
        elif x > 1e12:
            dt = pd.to_datetime(val, unit="ms", errors="coerce")
        elif x > 1e9:
            dt = pd.to_datetime(val, unit="s", errors="coerce")
        else:
            dt = pd.to_datetime(val, errors="coerce")
    else:
        dt = pd.to_datetime(val, errors="coerce")
    if pd.isna(dt):
        return None
    return dt


def _public_date_iso(row: pd.Series, colmap: dict[str, str]) -> str | None:
    for key in ("public_date", "publish_time", "created_at", "updated_at"):
        c = colmap.get(key)
        if not c:
            continue
        val = row.get(c)
        dt = _parse_ts_cell(val)
        if dt is None:
            continue
        try:
            return dt.isoformat()
        except Exception:
            return str(val)
    return None


def _title(row: pd.Series, colmap: dict[str, str]) -> str:
    for key in ("news_title", "title"):
        c = colmap.get(key)
        if c:
            v = row.get(c)
            if v is not None and not (isinstance(v, float) and pd.isna(v)):
                return str(v).strip()
    return ""


def _pdf_urls_from_html(html: str | None) -> list[str]:
    if html is None or (isinstance(html, float) and pd.isna(html)):
        return []
    soup = BeautifulSoup(str(html), "html.parser")
    out: list[str] = []
    for a in soup.find_all("a", href=True):
        h = str(a.get("href", "")).strip()
        if ".pdf" in h.lower():
            out.append(normalize_url(h))
    return [u for u in out if u.startswith("http")]


def _column_map(df: pd.DataFrame) -> dict[str, str]:
    lower = {str(c).lower(): c for c in df.columns}
    return {k: lower[k] for k in lower}


def discover_bctc_rows_vnstock(
    symbol: str,
    cfg: BctcPdfConfig,
    *,
    extra_keywords: tuple[str, ...] | None = None,
) -> list[dict[str, Any]]:
    """Return metadata dicts for PDF links found in VCI-style company news HTML."""
    end = date.today()
    cutoff = end - relativedelta(months=3 * max(1, int(cfg.quarters_back)))
    rows_out: list[dict[str, Any]] = []
    seen_url: set[str] = set()

    for src in cfg.vnstock_discovery_sources:
        src_l = str(src).strip().lower()
        if not src_l:
            continue
        try:
            df = _fetch_company_news_paginated(symbol, src_l, cfg)
        except Exception as ex:
            LOGGER.warning("bctc discovery %s @%s: %s", symbol, src_l, ex)
            continue

        if df is None or not isinstance(df, pd.DataFrame) or df.empty:
            continue

        cmap = _column_map(df)
        content_cols = [
            cmap[c]
            for c in ("news_full_content", "news_short_content")
            if c in cmap
        ]

        for _, row in df.iterrows():
            title = _title(row, cmap)
            pub_iso = _public_date_iso(row, cmap)
            pub_date: date | None = None
            if pub_iso:
                pdt = pd.to_datetime(pub_iso, errors="coerce")
                if pd.notna(pdt):
                    try:
                        pub_date = pdt.date()
                    except Exception:
                        pub_date = None
            if pub_date is not None and pub_date < cutoff:
                continue

            pdf_urls: list[str] = []
            for col in content_cols:
                pdf_urls.extend(_pdf_urls_from_html(row.get(col)))
            for raw_url in pdf_urls:
                u = normalize_url(raw_url)
                if not u or u in seen_url:
                    continue
                if not is_bctc_candidate(title, u, extra_keywords):
                    continue
                seen_url.add(u)
                year, quarter = (0, 0)
                if pub_date:
                    year, quarter = pub_date.year, (pub_date.month - 1) // 3 + 1
                rows_out.append(
                    {
                        "symbol": symbol.upper().strip(),
                        "title": title,
                        "published_at": pub_iso,
                        "source_url": u,
                        "year": year,
                        "quarter": quarter,
                        "discovery_source": f"vnstock_{src_l}",
                    }
                )
        if rows_out:
            break

    return rows_out
