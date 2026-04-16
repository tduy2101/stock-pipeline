"""
Legacy news schema kept for backward compatibility with existing notebooks.

The canonical schemas are now in :mod:`schemas` (``DISCOVERY_COLUMNS`` and
``ARTICLE_COLUMNS``).  Functions here delegate to :mod:`schemas` where
possible.

**Key change**: ``finalize_news_frame`` no longer fills ``body_text`` from
``summary`` or ``title``.  This was the root cause of "fake body" data.
"""

from __future__ import annotations

import hashlib
import logging
import re
import warnings
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse, urlunparse

import pandas as pd

from .schemas import (
    canonicalize_url,
    compact_text,
    na_safe_str,
    strip_html,
)

LOGGER = logging.getLogger(__name__)

_NEWS_WS = re.compile(r"\s+")

NEWS_COLUMNS: list[str] = [
    "article_id",
    "source",
    "ticker",
    "title",
    "summary",
    "body_text",
    "url",
    "published_at",
    "fetched_at",
    "language",
    "raw_ref",
]


def empty_news_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=NEWS_COLUMNS)


# ── URL normalization ───────────────────────────────────────────────────────

def normalize_url(url: str | None) -> str:
    """Normalize a URL **without** hardcoding any domain.

    .. deprecated::
       Use :func:`schemas.canonicalize_url` with an explicit ``base_url``
       instead. This wrapper calls ``canonicalize_url(url, base_url=None)``
       which drops purely-relative paths (returning ``""``).
    """
    return canonicalize_url(url, base_url=None)


# ── Text helpers (re-exported from schemas) ─────────────────────────────────

_compact_text = compact_text
_na_safe_str = na_safe_str
strip_html_to_text = strip_html


# ── Article ID ──────────────────────────────────────────────────────────────

def compute_article_id(
    *,
    url: Any = "",
    title: Any = "",
    published_at: Any = "",
    ticker: Any = "",
) -> str:
    """Legacy hash ID (includes ticker in the payload)."""
    norm_u = canonicalize_url(na_safe_str(url))
    t = compact_text(na_safe_str(title))
    p = compact_text(na_safe_str(published_at))
    sym = compact_text(na_safe_str(ticker)).upper()
    payload = f"{norm_u}\n{t}\n{p}\n{sym}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


# ── Timestamp parsing ──────────────────────────────────────────────────────

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


# ── Dedup ───────────────────────────────────────────────────────────────────

def dedupe_news(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if "article_id" not in out.columns:
        return out.drop_duplicates()
    return out.drop_duplicates(subset=["article_id"], keep="first")


# ── Helpers ─────────────────────────────────────────────────────────────────

def _is_blank_series(s: pd.Series) -> pd.Series:
    t = s.astype(str).str.strip()
    return t.isna() | (t == "") | (t.str.lower() == "nan") | (t.str.lower() == "<na>")


# ── Finalize (PATCHED: no longer fakes body_text) ──────────────────────────

def finalize_news_frame(
    df: pd.DataFrame, *, run_fetched_at: str | None = None
) -> pd.DataFrame:
    """Clean and finalize a legacy NEWS_COLUMNS DataFrame.

    **Breaking change**: ``body_text`` is **no longer** back-filled from
    ``summary`` or ``title``.  Empty ``body_text`` stays empty — the
    canonical pipeline fills it only when real content is available.
    """
    if df.empty:
        return empty_news_frame()
    out = df.copy()
    fetched = run_fetched_at or datetime.now(timezone.utc).isoformat()
    for col in NEWS_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA
    out["fetched_at"] = fetched
    out["url"] = out["url"].map(lambda x: canonicalize_url(x) if pd.notna(x) else "")

    sum_blank = _is_blank_series(out["summary"])
    out.loc[sum_blank, "summary"] = out.loc[sum_blank, "title"].map(compact_text)

    rr_blank = _is_blank_series(out["raw_ref"])
    out.loc[rr_blank, "raw_ref"] = out.loc[rr_blank, "url"].astype(str)

    article_ids: list[str] = []
    for _, r in out.iterrows():
        existing = r["article_id"]
        if existing is not None and pd.notna(existing):
            s = compact_text(na_safe_str(existing))
            if s:
                article_ids.append(s)
                continue
        article_ids.append(
            compute_article_id(
                url=r["url"],
                title=r["title"],
                published_at=r["published_at"],
                ticker=r["ticker"],
            )
        )
    out["article_id"] = article_ids
    out = dedupe_news(out)
    return out[NEWS_COLUMNS]


# ── Recency filter ──────────────────────────────────────────────────────────

def filter_news_by_recency(
    df: pd.DataFrame,
    *,
    days_back: int,
    keep_undated: bool = False,
) -> pd.DataFrame:
    """Keep rows with ``published_at`` within *days_back* days (UTC)."""
    if df.empty or days_back <= 0 or "published_at" not in df.columns:
        return df
    cutoff = pd.Timestamp.now(tz=timezone.utc) - pd.Timedelta(days=int(days_back))
    dt = pd.to_datetime(df["published_at"], errors="coerce", utc=True)
    undated = dt.isna()
    recent = dt >= cutoff
    if keep_undated:
        keep = recent | undated
    else:
        keep = recent
        if "source" in df.columns:
            src = df["source"].astype(str)
            html_undated = undated & src.str.startswith("html_")
            keep = keep | html_undated
    return df.loc[keep.fillna(False)].copy()
