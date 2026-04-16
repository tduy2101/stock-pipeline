"""
VnExpress detail-page fetcher.

Extracts article body from ``vnexpress.net`` detail pages using CSS selectors
for title, description, article body paragraphs, author, and publication date.
"""

from __future__ import annotations

import logging
import re
from typing import Any
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from ..schemas import compact_text, strip_html
from .base import DetailFetcher

LOGGER = logging.getLogger(__name__)

_VNEXPRESS_HOSTS = {"vnexpress.net", "www.vnexpress.net"}


class VnExpressDetailFetcher(DetailFetcher):
    """Extract article content from VnExpress detail pages."""

    def can_handle(self, url: str) -> bool:
        try:
            host = urlparse(url).hostname or ""
            return host.lower() in _VNEXPRESS_HOSTS
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

        title_el = soup.select_one("h1.title-detail") or soup.select_one("h1")
        if title_el:
            result["title"] = compact_text(title_el.get_text())

        desc_el = soup.select_one("p.description")
        if desc_el:
            result["summary"] = compact_text(desc_el.get_text())

        body_el = soup.select_one("article.fck_detail")
        if body_el:
            paragraphs: list[str] = []
            for p in body_el.find_all("p", class_=lambda c: c != "author_mail"):
                text = compact_text(p.get_text())
                if text and len(text) > 1:
                    paragraphs.append(text)
            result["body_text"] = "\n".join(paragraphs)

        author_el = soup.select_one("p.author_mail") or soup.select_one(
            "p.Normal[style*='right']"
        )
        if author_el:
            result["author"] = compact_text(author_el.get_text())

        time_el = soup.select_one("span.date") or soup.select_one("meta[name='pubdate']")
        if time_el:
            raw_time = time_el.get("content") or time_el.get_text()
            if raw_time:
                import pandas as pd
                parsed = pd.to_datetime(raw_time, errors="coerce")
                if pd.notna(parsed):
                    result["published_at"] = parsed.isoformat()

        tag_els = soup.select("meta[name='keywords']")
        if tag_els:
            kw = tag_els[0].get("content", "")
            if kw:
                result["tags"] = kw

        return result
