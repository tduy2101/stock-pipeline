"""
HTML list page adapter — **Discovery tier only**.

Fetches static HTML list pages (e.g. VnExpress category page) and extracts
article links + titles.  Does **not** set ``body_text`` — that is the job of
the detail-fetch tier.
"""

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
from .schemas import (
    DISCOVERY_COLUMNS,
    canonicalize_url,
    compact_text,
    compute_discovery_id,
    empty_discovery_frame,
)

LOGGER = logging.getLogger(__name__)


class HtmlListAdapter:
    """Fetch static HTML list pages and emit **DISCOVERY** records.

    Each ``html_source`` spec in ``sources.yaml`` defines a ``list_url`` and
    ``link_css`` selector.  The adapter collects ``(title, url)`` pairs — no
    ``body_text`` is produced at this tier.
    """

    def __init__(
        self, cfg: NewsIngestionConfig, *, html_specs: list[dict[str, Any]]
    ) -> None:
        self.cfg = cfg
        self.html_specs = html_specs

    def fetch(self) -> pd.DataFrame:
        """Return a DataFrame with :data:`DISCOVERY_COLUMNS`."""
        if not self.cfg.enable_html or not self.html_specs:
            return empty_discovery_frame()

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
            list_url: str = str(spec.get("list_url") or "").strip()
            link_css: str = str(spec.get("link_css") or "").strip()
            source_label: str = str(
                spec.get("source_label") or spec.get("id") or "html"
            ).strip()
            section: str = str(spec.get("section") or spec.get("id") or "").strip()
            link_regex = spec.get("link_regex")
            base_url: str = str(spec.get("base_url") or list_url).strip()

            if not list_url or not link_css:
                LOGGER.warning(
                    "HtmlListAdapter: skip spec missing list_url or link_css: %s",
                    spec.get("id"),
                )
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
                LOGGER.warning(
                    "HtmlListAdapter: bad selector %r: %s", link_css, ex
                )
                continue

            pat = re.compile(str(link_regex)) if link_regex else None
            n = 0
            for a in anchors:
                if n >= max_per:
                    break
                href = a.get("href")
                if not href:
                    continue
                raw_url = urljoin(list_url, str(href).strip())
                curl = canonicalize_url(raw_url, base_url=base_url)
                if not curl or not curl.startswith("http"):
                    continue
                if pat and not pat.search(curl):
                    continue

                title = compact_text(a.get_text())
                source = source_label

                rows.append(
                    {
                        "discovery_id": compute_discovery_id(
                            curl, source, "", title
                        ),
                        "source": source,
                        "source_type": "html_list",
                        "section": section,
                        "title": title,
                        "summary": "",
                        "url": raw_url,
                        "canonical_url": curl,
                        "published_at": None,
                        "fetched_at": fetched_at,
                        "language": "vi",
                        "raw_ref": f"{list_url} | {curl}",
                    }
                )
                n += 1

            LOGGER.info(
                "HtmlListAdapter [%s]: collected %d links from %s",
                source_label,
                n,
                list_url,
            )

        if not rows:
            return empty_discovery_frame()
        return pd.DataFrame(rows, columns=DISCOVERY_COLUMNS)
