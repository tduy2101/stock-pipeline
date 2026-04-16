"""
Canonical schemas for the two-tier news ingestion pipeline.

Tier 1 — **Discovery**: lightweight records from list pages, RSS feeds, vnstock API.
Tier 2 — **Article**: enriched records with real ``body_text`` from detail pages.

All ID and hash functions use SHA-256 for deterministic, collision-resistant keys.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any
from urllib.parse import urljoin, urlparse, urlunparse

import pandas as pd

_WS_RE = re.compile(r"\s+")
_HTML_TAG_RE = re.compile(r"<[^>]+>")

# ── Discovery schema (output of adapters) ──────────────────────────────────

DISCOVERY_COLUMNS: list[str] = [
    "discovery_id",
    "source",
    "source_type",
    "section",
    "title",
    "summary",
    "url",
    "canonical_url",
    "published_at",
    "fetched_at",
    "language",
    "raw_ref",
]

# ── Article schema (canonical output for analysis / search) ─────────────────

ARTICLE_COLUMNS: list[str] = [
    "article_id",
    "discovery_id",
    "source",
    "source_type",
    "content_type",
    "ticker",
    "title",
    "summary",
    "body_text",
    "url",
    "canonical_url",
    "published_at",
    "fetched_at",
    "language",
    "author",
    "tags",
    "raw_ref",
    "content_hash",
]


def empty_discovery_frame() -> pd.DataFrame:
    """Return an empty DataFrame with the discovery schema."""
    return pd.DataFrame(columns=DISCOVERY_COLUMNS)


def empty_article_frame() -> pd.DataFrame:
    """Return an empty DataFrame with the article schema."""
    return pd.DataFrame(columns=ARTICLE_COLUMNS)


# ── Text utilities ──────────────────────────────────────────────────────────

def na_safe_str(val: Any) -> str:
    """Convert a possibly-NA/None value to a plain string (never raises)."""
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except (TypeError, ValueError):
        pass
    return str(val)


def compact_text(text: Any) -> str:
    """Collapse whitespace and strip sentinel strings like 'nan'."""
    if text is None:
        return ""
    try:
        if pd.isna(text):
            return ""
    except (TypeError, ValueError):
        pass
    s = str(text).replace("\r", " ").replace("\n", " ")
    s = _WS_RE.sub(" ", s).strip()
    if s.lower() in ("nan", "none", "<na>"):
        return ""
    return s


def strip_html(html: Any) -> str:
    """Remove all HTML tags and return clean text.

    Uses BeautifulSoup when available; falls back to a regex strip.
    """
    if html is None:
        return ""
    try:
        if pd.isna(html):
            return ""
    except (TypeError, ValueError):
        pass
    raw = str(html)
    if not raw.strip():
        return ""
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(raw, "html.parser")
        return compact_text(soup.get_text(" "))
    except Exception:
        return compact_text(_HTML_TAG_RE.sub(" ", raw))


# ── URL utilities ───────────────────────────────────────────────────────────

def canonicalize_url(url: str | None, base_url: str | None = None) -> str:
    """Resolve and normalize a URL.

    * Relative paths are joined against *base_url* (required when the URL is
      relative — no domain is assumed by default).
    * Protocol-relative ``//`` URLs are promoted to ``https:``.
    * Trailing slashes (except bare ``/``) and fragments are stripped.
    """
    if url is None:
        return ""
    s = na_safe_str(url).strip()
    if not s or s.lower() in ("nan", "none", "<na>"):
        return ""

    if s.startswith("//"):
        s = "https:" + s

    if not s.startswith(("http://", "https://")):
        if base_url:
            s = urljoin(base_url, s)
        else:
            return ""

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


# ── ID / hash helpers ───────────────────────────────────────────────────────

def _sha256(payload: str) -> str:
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def compute_discovery_id(
    canonical_url: str,
    source: str,
    published_at: str = "",
    title: str = "",
) -> str:
    """Deterministic discovery ID: ``sha256(canonical_url + source + published_at + title)``."""
    parts = [
        na_safe_str(canonical_url),
        na_safe_str(source),
        na_safe_str(published_at),
        compact_text(title),
    ]
    return _sha256("\n".join(parts))


def compute_article_id(
    canonical_url: str,
    source: str,
    published_at: str = "",
) -> str:
    """Deterministic article ID: ``sha256(canonical_url + source + published_at)``."""
    parts = [
        na_safe_str(canonical_url),
        na_safe_str(source),
        na_safe_str(published_at),
    ]
    return _sha256("\n".join(parts))


def compute_content_hash(body_text: str) -> str:
    """SHA-256 of normalized body text (for dedup / change detection)."""
    normalized = compact_text(body_text)
    return _sha256(normalized) if normalized else ""


# ── Disclosure detection ────────────────────────────────────────────────────

_DISCLOSURE_KEYWORDS = re.compile(
    r"(BCTC|CBTT|Nghị quyết|Báo cáo thường niên|Báo cáo tài chính|"
    r"Công bố thông tin|Đại hội đồng cổ đông|Biên bản họp|Biên bản kiểm phiếu|"
    r"Giải trình|Quyết định|Thông báo)",
    re.IGNORECASE,
)


def looks_like_disclosure(title: str) -> bool:
    """Heuristic: does the title look like a regulatory disclosure (CBTT)?"""
    return bool(_DISCLOSURE_KEYWORDS.search(na_safe_str(title)))
