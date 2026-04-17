"""HTML list adapter for one-layer news ingestion."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

from ingestion.common import call_with_retry, wait_for_rate_limit

from .config import NewsIngestionConfig
from .schema import (
    NEWS_COLUMNS,
    compact_text,
    compute_article_id,
    dedupe_news,
    empty_news_frame,
    normalize_url,
    safe_json_dumps,
)

LOGGER = logging.getLogger(__name__)


def fetch_html_list_news(
    cfg: NewsIngestionConfig, html_specs: list[dict[str, Any]]
) -> pd.DataFrame:
    if not html_specs:
        return empty_news_frame()
    fetched_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    rows: list[dict[str, str | None]] = []
    max_per = max(1, int(cfg.max_articles_per_source))

    for spec in html_specs:
        if not spec.get("enabled", False):
            continue
        list_url = compact_text(spec.get("list_url"))
        link_css = compact_text(spec.get("link_css"))
        source_label = compact_text(spec.get("source_label") or spec.get("id") or "html")
        source = f"html_{source_label}"
        if not list_url or not link_css:
            continue

        wait_for_rate_limit(cfg.rate_limit_rpm)

        def _get() -> str:
            r = requests.get(list_url, timeout=cfg.timeout_sec)
            r.raise_for_status()
            return r.text

        try:
            html_text = call_with_retry(
                _get,
                max_attempts=cfg.api_retry_max_attempts,
                base_delay_sec=cfg.api_retry_base_delay_sec,
                label=f"html:{source_label}",
            )
        except Exception as ex:
            LOGGER.warning("Failed HTML list %s: %s", list_url, ex)
            continue

        soup = BeautifulSoup(html_text, "html.parser")
        anchors = soup.select(link_css)
        for a in anchors[:max_per]:
            href = compact_text(a.get("href"))
            title = compact_text(a.get_text())
            if not href or not title:
                continue
            url = normalize_url(urljoin(list_url, href))
            if not url:
                continue
            article_id = compute_article_id(
                url=url,
                source=source,
                published_at="",
                ticker="",
                title=title,
            )
            rows.append(
                {
                    "article_id": article_id,
                    "source": source,
                    "ticker": None,
                    "title": title,
                    "summary": "",
                    "body_text": "",
                    "url": url,
                    "published_at": None,
                    "fetched_at": fetched_at,
                    "language": "vi",
                    "raw_ref": safe_json_dumps(
                        {
                            "list_url": list_url,
                            "href": href,
                            "css": link_css,
                            "source_label": source_label,
                        }
                    ),
                }
            )

    if not rows:
        return empty_news_frame()
    return dedupe_news(pd.DataFrame(rows, columns=NEWS_COLUMNS))
