from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse, urlunparse

import pandas as pd

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


def normalize_url(url: str | None) -> str:
    if url is None or (isinstance(url, float) and pd.isna(url)):
        return ""
    s = str(url).strip()
    if not s or s.lower() in ("nan", "none", "<na>"):
        return ""
    if s.startswith("//"):
        s = "https:" + s
    if not s.startswith(("http://", "https://")):
        s = "https://cafef.vn" + (s if s.startswith("/") else "/" + s)
    try:
        p = urlparse(s)
        path = p.path or ""
        if path != "/" and path.endswith("/"):
            path = path.rstrip("/")
        netloc = (p.hostname or "").lower()
        if p.port and p.port not in (80, 443):
            netloc = f"{netloc}:{p.port}"
        scheme = p.scheme.lower() if p.scheme else "https"
        if scheme not in ("http", "https"):
            scheme = "https"
        return urlunparse((scheme, netloc, path, "", p.query, ""))
    except Exception:
        return s


def _na_safe_str(val: Any) -> str:
    """Chuỗi hóa ô DataFrame/Series; tránh ``pd.NA or ''`` (ambiguous truth value)."""
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except TypeError:
        pass
    return str(val)


def _compact_text(text: str | None) -> str:
    if text is None or (isinstance(text, float) and pd.isna(text)):
        return ""
    s = str(text).replace("\r", " ").replace("\n", " ")
    s = _NEWS_WS.sub(" ", s).strip()
    if s.lower() in ("nan", "none", "<na>"):
        return ""
    return s


def strip_html_to_text(html: str | None) -> str:
    if not html or (isinstance(html, float) and pd.isna(html)):
        return ""
    raw = str(html)
    try:
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(raw, "html.parser")
        return _compact_text(soup.get_text(" "))
    except Exception:
        return _compact_text(re.sub(r"<[^>]+>", " ", raw))


def compute_article_id(
    *,
    url: Any = "",
    title: Any = "",
    published_at: Any = "",
    ticker: Any = "",
) -> str:
    """Hash id; mọi tham số được đưa qua ``_na_safe_str`` (tránh ``pd.NA`` từ RSS/HTML)."""
    norm_u = normalize_url(_na_safe_str(url))
    t = _compact_text(_na_safe_str(title))
    p = _compact_text(_na_safe_str(published_at))
    sym = _compact_text(_na_safe_str(ticker)).upper()
    payload = f"{norm_u}\n{t}\n{p}\n{sym}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


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


def dedupe_news(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if "article_id" not in out.columns:
        return out.drop_duplicates()
    return out.drop_duplicates(subset=["article_id"], keep="first")


def _is_blank_series(s: pd.Series) -> pd.Series:
    t = s.astype(str).str.strip()
    return t.isna() | (t == "") | (t.str.lower() == "nan") | (t.str.lower() == "<na>")


def finalize_news_frame(df: pd.DataFrame, *, run_fetched_at: str | None = None) -> pd.DataFrame:
    if df.empty:
        return empty_news_frame()
    out = df.copy()
    fetched = run_fetched_at or datetime.now(timezone.utc).isoformat()
    for col in NEWS_COLUMNS:
        if col not in out.columns:
            out[col] = pd.NA
    # Một lần chạy = một mốc fetched_at cho toàn bộ dòng (tránh chuỗi rỗng từ adapter).
    out["fetched_at"] = fetched
    out["url"] = out["url"].map(lambda x: normalize_url(x) if pd.notna(x) else "")
    # summary / body_text: luôn có nội dung khi có title hoặc url.
    sum_blank = _is_blank_series(out["summary"])
    out.loc[sum_blank, "summary"] = out.loc[sum_blank, "title"].map(_compact_text)
    body_blank = _is_blank_series(out["body_text"])
    out.loc[body_blank, "body_text"] = out.loc[body_blank, "summary"].map(_compact_text)
    # raw_ref: tham chiếu nguồn (feed|url đã set ở RSS; vnstock dùng url bài).
    rr_blank = _is_blank_series(out["raw_ref"])
    out.loc[rr_blank, "raw_ref"] = out.loc[rr_blank, "url"].astype(str)
    article_ids: list[str] = []
    for _, r in out.iterrows():
        existing = r["article_id"]
        if existing is not None and pd.notna(existing):
            s = _compact_text(_na_safe_str(existing))
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


def filter_news_by_recency(
    df: pd.DataFrame,
    *,
    days_back: int,
    keep_undated: bool = False,
) -> pd.DataFrame:
    """Giữ bài có ``published_at`` trong ``days_back`` ngày gần nhất (UTC)."""
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
        # Trang list HTML thường không có ``published_at`` — vẫn giữ để tránh mất cả nhánh HTML.
        if "source" in df.columns:
            src = df["source"].astype(str)
            html_undated = undated & src.str.startswith("html_")
            keep = keep | html_undated
    out = df.loc[keep.fillna(False)].copy()
    return out
