"""
Base class for detail-page fetchers.

Each fetcher handles a specific domain (or set of domains) and knows how to
extract structured article content from an HTML detail page.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any

import requests

from ingestion.common import call_with_retry, wait_for_rate_limit

LOGGER = logging.getLogger(__name__)

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
}


class DetailFetcher(ABC):
    """Interface for domain-specific article detail extractors.

    Subclasses must implement :meth:`can_handle` and :meth:`fetch_one`.
    """

    def __init__(
        self,
        *,
        rate_limit_rpm: int = 30,
        timeout_sec: int = 30,
        retry_max: int = 3,
        retry_base_delay: float = 1.5,
    ) -> None:
        self.rate_limit_rpm = rate_limit_rpm
        self.timeout_sec = timeout_sec
        self.retry_max = retry_max
        self.retry_base_delay = retry_base_delay
        self._session = requests.Session()
        self._session.headers.update(DEFAULT_HEADERS)

    @abstractmethod
    def can_handle(self, url: str) -> bool:
        """Return True if this fetcher can parse the given URL's detail page."""

    @abstractmethod
    def _parse_html(self, html: str, url: str) -> dict[str, Any]:
        """Extract article fields from raw HTML.

        Must return a dict with at least:
        ``title``, ``summary``, ``body_text``, ``author``, ``published_at``, ``tags``.
        Missing fields should be ``""``.
        """

    def fetch_one(self, discovery_row: dict[str, Any]) -> dict[str, Any]:
        """Fetch and parse a single article, returning ARTICLE_COLUMNS fields.

        Returns partial dict; the caller is responsible for merging with
        discovery data and computing IDs.
        """
        url = discovery_row.get("canonical_url") or discovery_row.get("url", "")
        if not url:
            return {"body_text": "", "author": "", "tags": ""}

        wait_for_rate_limit(self.rate_limit_rpm)

        def _get() -> requests.Response:
            return self._session.get(url, timeout=self.timeout_sec)

        try:
            resp = call_with_retry(
                _get,
                max_attempts=self.retry_max,
                base_delay_sec=self.retry_base_delay,
                label=f"detail:{url[:80]}",
            )
            resp.raise_for_status()
            resp.encoding = resp.apparent_encoding or "utf-8"
            return self._parse_html(resp.text, url)
        except Exception as ex:
            LOGGER.warning("DetailFetcher: failed to fetch %s: %s", url, ex)
            return {"body_text": "", "author": "", "tags": ""}
