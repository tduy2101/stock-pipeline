"""
CafeF detail-page fetcher.

Extracts article body from ``cafef.vn`` detail pages.  CafeF layout varies
across sections; we try multiple selector strategies and fall back gracefully.
"""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from ..schemas import compact_text
from .base import DetailFetcher

LOGGER = logging.getLogger(__name__)

_CAFEF_HOSTS = {"cafef.vn", "www.cafef.vn", "s.cafef.vn"}


class CafeFDetailFetcher(DetailFetcher):
    """Extract article content from CafeF detail pages."""

    def can_handle(self, url: str) -> bool:
        try:
            host = urlparse(url).hostname or ""
            return host.lower() in _CAFEF_HOSTS
        except Exception:
            return False

    def _parse_html(self, html: str, url: str) -> dict[str, Any]:
        soup = BeautifulSoup(html, "html.parser")
        result: dict[str, Any] = {
            "title": "",
            "summary": "",
            "body_text": "",
            "author": "",
            "published_at": "",
            "tags": "",
        }

        title_el = (
            soup.select_one("h1.title")
            or soup.select_one("h1#newsTitle")
            or soup.select_one("h1")
        )
        if title_el:
            result["title"] = compact_text(title_el.get_text())

        desc_el = soup.select_one("h2.sapo") or soup.select_one("p.sapo")
        if desc_el:
            result["summary"] = compact_text(desc_el.get_text())

        body_candidates = [
            soup.select_one("div#mainContent"),
            soup.select_one("div.detail-content"),
            soup.select_one("div#contentdetail"),
            soup.select_one("div.knc-content"),
        ]
        body_el = next((el for el in body_candidates if el), None)

        if body_el:
            paragraphs: list[str] = []
            for p in body_el.find_all(["p", "div"], recursive=True):
                if p.find("p"):
                    continue
                text = compact_text(p.get_text())
                if text and len(text) > 5:
                    paragraphs.append(text)
            result["body_text"] = "\n".join(paragraphs)

        if not result["body_text"]:
            LOGGER.info(
                "CafeFDetailFetcher: could not extract body from %s", url
            )

        author_el = soup.select_one("p.author") or soup.select_one("span.author")
        if author_el:
            result["author"] = compact_text(author_el.get_text())

        time_el = soup.select_one("span.pdate") or soup.select_one(
            "meta[name='pubdate']"
        )
        if time_el:
            raw_time = time_el.get("content") or time_el.get_text()
            if raw_time:
                import pandas as pd
                parsed = pd.to_datetime(raw_time, errors="coerce")
                if pd.notna(parsed):
                    result["published_at"] = parsed.isoformat()

        return result
