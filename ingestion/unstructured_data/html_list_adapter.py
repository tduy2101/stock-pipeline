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
    build_ticker_regex,
    compact_text,
    compute_article_id,
    dedupe_news,
    empty_news_frame,
    infer_ticker,
    normalize_url,
    parse_datetime_to_iso_utc,
    safe_json_dumps,
    strip_html,
)

LOGGER = logging.getLogger(__name__)


def _select_one_text(soup: BeautifulSoup, css: str | None) -> str:
    if not css:
        return ""
    node = soup.select_one(css)
    if not node:
        return ""
    return compact_text(node.get_text(" "))


def _select_many_text(soup: BeautifulSoup, css: str | None) -> str:
    if not css:
        return ""
    nodes = soup.select(css)
    if not nodes:
        return ""
    return compact_text(" ".join(n.get_text(" ") for n in nodes))


def fetch_html_list_news(
    cfg: NewsIngestionConfig, html_specs: list[dict[str, Any]]
) -> pd.DataFrame:
    if not html_specs:
        return empty_news_frame()
    fetched_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    rows: list[dict[str, str | None]] = []
    max_per = int(getattr(cfg, "html_max_per_source", 0) or cfg.max_articles_per_source)
    max_per = max(1, max_per)

    ticker_re = (
        build_ticker_regex(cfg.resolved_tickers()) if cfg.enable_ticker_match else None
    )
    session = requests.Session()
    session.headers.update(cfg.http_headers)

    for spec in html_specs:
        if spec.get("enabled", True) is False:
            continue
        list_url = compact_text(spec.get("list_url"))
        link_css = compact_text(spec.get("link_css"))
        source_label = compact_text(spec.get("source_label") or spec.get("id") or "html")
        source = f"{source_label}_html" if source_label else "html"
        extra_headers = spec.get("headers") or {}
        if isinstance(extra_headers, dict) and extra_headers:
            merged_headers = {
                **cfg.http_headers,
                **{
                    str(k): str(v)
                    for k, v in extra_headers.items()
                    if v is not None
                },
            }
        else:
            merged_headers = cfg.http_headers
        if not list_url or not link_css:
            continue

        wait_for_rate_limit(cfg.rate_limit_rpm)

        def _get() -> str:
            r = session.get(list_url, timeout=cfg.timeout_sec, headers=merged_headers)
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
        LOGGER.info(
            "HTML list %s: links_found=%d (take<=%d)",
            list_url,
            len(anchors),
            max_per,
        )
        detail_cfg = spec.get("detail") or {}
        detail_success = 0
        for a in anchors[:max_per]:
            href = compact_text(a.get("href"))
            title = compact_text(a.get_text())
            if not href or not title:
                continue
            url = normalize_url(urljoin(list_url, href))
            if not url:
                continue
            summary = ""
            body_text = ""
            published_at = None
            if detail_cfg:
                wait_for_rate_limit(cfg.rate_limit_rpm)

                def _get_detail() -> str:
                    r = session.get(url, timeout=cfg.timeout_sec, headers=merged_headers)
                    r.raise_for_status()
                    return r.text

                try:
                    detail_html = call_with_retry(
                        _get_detail,
                        max_attempts=cfg.api_retry_max_attempts,
                        base_delay_sec=cfg.api_retry_base_delay_sec,
                        label=f"html_detail:{source_label}",
                    )
                    detail_soup = BeautifulSoup(detail_html, "html.parser")
                    detail_title = _select_one_text(detail_soup, detail_cfg.get("title_css"))
                    detail_summary = _select_one_text(detail_soup, detail_cfg.get("summary_css"))
                    detail_body = _select_many_text(detail_soup, detail_cfg.get("body_css"))
                    detail_published = _select_one_text(
                        detail_soup, detail_cfg.get("published_at_css")
                    )
                    title = detail_title or title
                    summary = strip_html(detail_summary)
                    body_text = strip_html(detail_body)
                    published_at = parse_datetime_to_iso_utc(detail_published)
                    detail_success += 1
                except Exception as ex:
                    LOGGER.debug("Failed HTML detail %s: %s", url, ex)
            ticker = infer_ticker([title, summary, body_text], ticker_re)
            article_id = compute_article_id(
                url=url,
                source=source,
                published_at=published_at or "",
                ticker=ticker or "",
                title=title,
            )
            rows.append(
                {
                    "article_id": article_id,
                    "source": source,
                    "ticker": ticker,
                    "title": title,
                    "summary": summary,
                    "body_text": body_text,
                    "url": url,
                    "published_at": published_at,
                    "fetched_at": fetched_at,
                    "language": "vi",
                    "raw_ref": safe_json_dumps(
                        {
                            "list_url": list_url,
                            "href": href,
                            "css": link_css,
                            "source_label": source_label,
                            "detail": detail_cfg,
                        }
                    ),
                }
            )

        if detail_cfg:
            LOGGER.info(
                "HTML list %s: detail_success=%d/%d",
                list_url,
                detail_success,
                min(len(anchors), max_per),
            )

    if not rows:
        return empty_news_frame()
    return dedupe_news(pd.DataFrame(rows, columns=NEWS_COLUMNS))
