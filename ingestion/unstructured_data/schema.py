from __future__ import annotations

import hashlib
import json
import logging
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import pandas as pd

LOGGER = logging.getLogger(__name__)

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

_WS_RE = re.compile(r"\s+")
_HTML_TAG_RE = re.compile(r"<[^>]+>")


def compact_text(text: Any) -> str:
    if text is None:
        return ""
    try:
        if pd.isna(text):
            return ""
    except (TypeError, ValueError):
        pass
    val = str(text).replace("\r", " ").replace("\n", " ")
    val = _WS_RE.sub(" ", val).strip()
    if val.lower() in {"nan", "none", "<na>"}:
        return ""
    return val


def strip_html(text: Any) -> str:
    raw = compact_text(text)
    if not raw:
        return ""
    try:
        from bs4 import BeautifulSoup

        return compact_text(BeautifulSoup(raw, "html.parser").get_text(" "))
    except Exception:
        return compact_text(_HTML_TAG_RE.sub(" ", raw))


def normalize_url(url: Any) -> str:
    raw = compact_text(url)
    if not raw:
        return ""
    if raw.startswith("//"):
        raw = "https:" + raw
    if not raw.startswith(("http://", "https://")):
        return ""
    try:
        parsed = urlparse(raw)
        scheme = parsed.scheme.lower() if parsed.scheme else "https"
        if scheme not in ("http", "https"):
            scheme = "https"
        host = (parsed.hostname or "").lower()
        if parsed.port and parsed.port not in (80, 443):
            host = f"{host}:{parsed.port}"
        path = parsed.path or ""
        if path != "/" and path.endswith("/"):
            path = path.rstrip("/")
        query_items = [(k, v) for k, v in parse_qsl(parsed.query, keep_blank_values=True) if not k.startswith("utm_")]
        query = urlencode(query_items)
        return urlunparse((scheme, host, path, "", query, ""))
    except Exception:
        return raw


def parse_datetime_to_iso_utc(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        ts = value
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        else:
            ts = ts.astimezone(timezone.utc)
        return ts.isoformat().replace("+00:00", "Z")
    if hasattr(value, "tm_year"):
        try:
            ts = datetime(*value[:6], tzinfo=timezone.utc)
            return ts.isoformat().replace("+00:00", "Z")
        except Exception:
            pass
    if isinstance(value, (int, float)):
        try:
            if pd.isna(value):
                return None
        except (TypeError, ValueError):
            pass
        unit = "ms" if abs(float(value)) > 10_000_000_000 else "s"
        ts = pd.to_datetime(value, errors="coerce", utc=True, unit=unit)
        if not pd.isna(ts):
            return ts.tz_convert(timezone.utc).isoformat().replace("+00:00", "Z")
    text = compact_text(value)
    if not text:
        return None
    ts = pd.to_datetime(text, errors="coerce", utc=True)
    if pd.isna(ts):
        return None
    return ts.tz_convert(timezone.utc).isoformat().replace("+00:00", "Z")


def compute_article_id(
    *,
    url: str,
    source: str,
    published_at: str | None,
    ticker: str | None,
    title: str,
) -> str:
    norm_url = normalize_url(url)
    if norm_url:
        payload = norm_url
    else:
        payload = "\n".join(
            [
                compact_text(source),
                compact_text(published_at),
                compact_text(ticker),
                compact_text(title),
            ]
        )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def build_ticker_regex(tickers: list[str]) -> re.Pattern | None:
    cleaned: list[str] = []
    for t in tickers:
        text = compact_text(t).upper()
        if not text or text in {"NAN", "NONE", "<NA>"}:
            continue
        if len(text) < 2:
            continue
        cleaned.append(text)
    if not cleaned:
        return None
    cleaned = sorted(set(cleaned), key=len, reverse=True)
    pattern = r"(?<![A-Z0-9])(" + "|".join(map(re.escape, cleaned)) + r")(?![A-Z0-9])"
    return re.compile(pattern)


def infer_ticker(texts: list[Any], regex: re.Pattern | None) -> str | None:
    if regex is None:
        return None
    for text in texts:
        raw = compact_text(text).upper()
        if not raw:
            continue
        match = regex.search(raw)
        if match:
            return match.group(1)
    return None


def safe_json_dumps(data: Any) -> str:
    try:
        return json.dumps(data, ensure_ascii=False, default=str)
    except Exception:
        return json.dumps({"raw": str(data)}, ensure_ascii=False)


def empty_news_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=NEWS_COLUMNS)


def dedupe_news(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return empty_news_frame()
    out = df.copy()
    out = out.drop_duplicates(subset=["article_id"], keep="first")
    return out[NEWS_COLUMNS].reset_index(drop=True)


def validate_news_df(df: pd.DataFrame) -> list[str]:
    issues: list[str] = []
    missing = [c for c in NEWS_COLUMNS if c not in df.columns]
    if missing:
        issues.append(f"Missing columns: {missing}")
        return issues
    if df.empty:
        return issues

    article_id = df["article_id"].fillna("").astype(str).str.strip()
    if (article_id == "").any():
        issues.append("article_id has empty values")

    urls = df["url"].fillna("").astype(str).str.strip()
    if (~urls.str.startswith("http")).any():
        issues.append("url must start with http")

    titles = df["title"].fillna("").astype(str).str.strip()
    if (titles == "").any():
        issues.append("title has empty values")

    body = df["body_text"].fillna("").astype(str).str.strip()
    same = (body != "") & (body == titles)
    ratio = float(same.mean()) if len(df) else 0.0
    if ratio > 0.2:
        LOGGER.warning("body_text == title ratio is high: %.2f%% (%d/%d)", ratio * 100, int(same.sum()), len(df))
    return issues
