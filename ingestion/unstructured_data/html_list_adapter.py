"""HTML list adapter for one-layer news ingestion."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

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
    infer_ticker_with_universe,
    normalize_url,
    safe_json_dumps,
    strip_html,
)

LOGGER = logging.getLogger(__name__)

SOURCE_DATE_CONFIG = {
    "cafef_html": {
        "selector": "span.pdate",
        "format": "%d-%m-%Y - %I:%M %p",
    },
    "vnexpress_html": {
        "selector": "span.date",
        "format": "%A, %d/%m/%Y, %H:%M (GMT+7)",
        "locale": "vi_VN",
    },
}

SOURCE_DATE_SCOPES = {
    "cafef_html": (
        "article",
        "div.detail-section",
        "div.main",
        "div.left_cate.totalcontentdetail",
        "div.totalcontentdetail",
    ),
    "vnexpress_html": (
        "header",
        "article",
        "div.header-content",
        "section.page-detail",
        "div.sidebar-1",
    ),
}

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")


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


def _select_published_at_raw(soup: BeautifulSoup, source: str) -> str:
    cfg = SOURCE_DATE_CONFIG.get(source)
    if not cfg:
        return ""
    selector = str(cfg["selector"])
    for scope_selector in SOURCE_DATE_SCOPES.get(source, ()):
        scope = soup.select_one(scope_selector)
        if not scope:
            continue
        tag = scope.select_one(selector)
        if tag:
            raw = compact_text(tag.get_text(" ", strip=True))
            if raw:
                return raw
    tag = soup.select_one(selector)
    if not tag:
        return ""
    return compact_text(tag.get_text(" ", strip=True))


def _parse_vnexpress_date(raw: str) -> datetime | None:
    cleaned = re.sub(r"^[^,]+,\s*", "", raw.strip())
    cleaned = re.sub(r"\s*\(GMT\+7\)$", "", cleaned)
    return datetime.strptime(cleaned, "%d/%m/%Y, %H:%M")


def _parse_cafef_date(raw: str, fmt: str) -> datetime:
    try:
        return datetime.strptime(raw, fmt)
    except ValueError:
        cleaned = re.sub(r"\s+(?:AM|PM)$", "", raw.strip(), flags=re.IGNORECASE)
        return datetime.strptime(cleaned, "%d-%m-%Y - %H:%M")


def parse_published_at(soup: BeautifulSoup, source: str) -> datetime | None:
    raw = ""
    try:
        cfg = SOURCE_DATE_CONFIG.get(source)
        if not cfg:
            return None
        raw = _select_published_at_raw(soup, source)
        if not raw:
            return None
        if source == "vnexpress_html":
            local_naive = _parse_vnexpress_date(raw)
        elif source == "cafef_html":
            local_naive = _parse_cafef_date(raw, str(cfg["format"]))
        else:
            local_naive = datetime.strptime(raw, str(cfg["format"]))
        if local_naive is None:
            return None
        return local_naive.replace(tzinfo=VN_TZ).astimezone(timezone.utc)
    except Exception as ex:
        LOGGER.warning(
            "parse_published_at failed | source=%s | raw=%r | %s",
            source,
            raw,
            ex,
        )
        return None


def fetch_html_list_news(
    cfg: NewsIngestionConfig, html_specs: list[dict[str, Any]]
) -> pd.DataFrame:
    if not html_specs:
        return empty_news_frame()
    fetched_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    rows: list[dict[str, str | None]] = []
    max_per = int(getattr(cfg, "html_max_per_source", 0) or cfg.max_articles_per_source)
    max_per = max(1, max_per)

    ticker_universe = cfg.resolved_ticker_universe() if cfg.enable_ticker_match else frozenset()
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
        anchors_found = len(anchors)
        detail_cfg = spec.get("detail") or {}
        detail_success = 0
        rows_before = len(rows)
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
                    title = detail_title or title
                    summary = strip_html(detail_summary)
                    body_text = strip_html(detail_body)
                    published_at = parse_published_at(detail_soup, source)
                    detail_success += 1
                except Exception as ex:
                    LOGGER.debug("Failed HTML detail %s: %s", url, ex)
            if detail_cfg and not published_at and not body_text:
                continue
            ticker = infer_ticker_with_universe(
                [title, summary, body_text],
                ticker_universe,
            )
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

        rows_added = len(rows) - rows_before
        LOGGER.info(
            "HTML source=%s anchors_found=%d detail_success=%d rows_added=%d take<=%d",
            list_url,
            anchors_found,
            detail_success if detail_cfg else 0,
            rows_added,
            max_per,
        )

    if not rows:
        return empty_news_frame()
    return dedupe_news(pd.DataFrame(rows, columns=NEWS_COLUMNS))
