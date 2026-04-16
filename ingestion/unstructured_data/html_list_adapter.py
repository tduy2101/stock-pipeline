from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

from ingestion.common import call_with_retry, wait_for_rate_limit

from .config import NewsIngestionConfig
from .news_schema import NEWS_COLUMNS, _compact_text, empty_news_frame, normalize_url

LOGGER = logging.getLogger(__name__)


class HtmlListAdapter:
    """Fetch a static HTML list page and collect article links via CSS selector."""

    def __init__(self, cfg: NewsIngestionConfig, *, html_specs: list[dict[str, Any]]) -> None:
        self.cfg = cfg
        self.html_specs = html_specs

    def fetch(self) -> pd.DataFrame:
        if not self.cfg.enable_html or not self.html_specs:
            return empty_news_frame()

        rows: list[dict[str, Any]] = []
        fetched_at = datetime.now(timezone.utc).isoformat()
        max_per = max(1, self.cfg.max_articles_per_source)
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": "stock-pipeline-news/1.0 (research; +https://github.com/)",
                "Accept": "text/html,application/xhtml+xml",
            }
        )

        for spec in self.html_specs:
            if not spec.get("enabled", False):
                continue
            list_url = str(spec.get("list_url") or "").strip()
            link_css = str(spec.get("link_css") or "").strip()
            source_label = str(spec.get("source_label") or spec.get("id") or "html").strip()
            link_regex = spec.get("link_regex")
            if not list_url or not link_css:
                LOGGER.warning("HtmlListAdapter: skip spec missing list_url or link_css: %s", spec.get("id"))
                continue

            wait_for_rate_limit(self.cfg.rate_limit_rpm)

            def _get() -> requests.Response:
                wait_for_rate_limit(self.cfg.rate_limit_rpm)
                return session.get(list_url, timeout=30)

            try:
                resp = call_with_retry(
                    _get,
                    max_attempts=self.cfg.api_retry_max_attempts,
                    base_delay_sec=self.cfg.api_retry_base_delay_sec,
                    label=f"html_list:{source_label}",
                )
                resp.raise_for_status()
            except Exception as ex:
                LOGGER.warning("HtmlListAdapter: GET %s failed: %s", list_url, ex)
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            try:
                anchors = soup.select(link_css)
            except Exception as ex:
                LOGGER.warning("HtmlListAdapter: bad selector %r: %s", link_css, ex)
                continue

            pat = re.compile(str(link_regex)) if link_regex else None
            n = 0
            for a in anchors:
                if n >= max_per:
                    break
                href = a.get("href")
                if not href:
                    continue
                url = normalize_url(urljoin(list_url, str(href).strip()))
                if not url or not url.startswith("http"):
                    continue
                if pat and not pat.search(url):
                    continue
                title = _compact_text(a.get_text())
                rows.append(
                    {
                        "article_id": "",
                        "source": f"html_{source_label}",
                        "ticker": pd.NA,
                        "title": title,
                        "summary": title,
                        "body_text": title,
                        "url": url,
                        "published_at": None,
                        "fetched_at": fetched_at,
                        "language": pd.NA,
                        "raw_ref": f"{list_url} | {url}",
                    }
                )
                n += 1

        if not rows:
            return empty_news_frame()
        return pd.DataFrame(rows, columns=NEWS_COLUMNS)
