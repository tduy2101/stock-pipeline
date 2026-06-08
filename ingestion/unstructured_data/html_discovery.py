"""Discover news list sections and suggest CSS selectors from live HTML."""

from __future__ import annotations

import argparse
import json
import logging
import re
from collections import Counter
from dataclasses import asdict, dataclass, field
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup, Tag

from ingestion.common import configure_logging

from .article_heuristics import extract_article_heuristic
from .config import NewsIngestionConfig
from .schema import compact_text, normalize_url

LOGGER = logging.getLogger(__name__)

_SECTION_KEYWORDS = (
    "kinh-doanh",
    "chung-khoan",
    "tai-chinh",
    "doanh-nghiep",
    "thi-truong",
    "ngan-hang",
    "co-phieu",
    "bat-dong-san",
    "smart-money",
    "nhan-dinh",
    "phan-tich",
    "chn",
    "htm",
)
_NOISE_HREF_RE = re.compile(
    r"(login|register|javascript:|mailto:|#|facebook|youtube|tiktok|/tag/|/tim-kiem|search)",
    re.IGNORECASE,
)


@dataclass
class SectionCandidate:
    title: str
    url: str
    score: float


@dataclass
class DiscoveryReport:
    input_url: str
    sections: list[SectionCandidate] = field(default_factory=list)
    suggested_link_css: str = ""
    suggested_detail: dict[str, str] = field(default_factory=dict)
    sample_article_urls: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


def _fetch_html(url: str, cfg: NewsIngestionConfig) -> str:
    session = requests.Session()
    session.headers.update(cfg.http_headers)
    response = session.get(url, timeout=cfg.timeout_sec)
    response.raise_for_status()
    return response.text


def _link_score(href: str, text: str, base_host: str) -> float:
    if not href or not text:
        return -1.0
    if _NOISE_HREF_RE.search(href):
        return -1.0
    parsed = urlparse(normalize_url(href) or href)
    if parsed.hostname and parsed.hostname.lower() not in {base_host, f"www.{base_host}"}:
        if not parsed.hostname.endswith(base_host):
            return -1.0
    score = min(len(text), 120) / 10.0
    path = (parsed.path or "").lower()
    if any(k in path for k in _SECTION_KEYWORDS):
        score += 3.0
    if path.endswith((".chn", ".htm", ".html")):
        score += 1.0
    return score


def discover_sections(html: str, base_url: str, *, limit: int = 20) -> list[SectionCandidate]:
    soup = BeautifulSoup(html, "html.parser")
    base_host = (urlparse(base_url).hostname or "").lower().removeprefix("www.")
    seen: set[str] = set()
    candidates: list[SectionCandidate] = []

    for anchor in soup.find_all("a", href=True):
        href = compact_text(anchor["href"])
        text = compact_text(anchor.get_text(" "))
        if len(text) < 4:
            continue
        url = normalize_url(urljoin(base_url, href))
        if not url or url in seen:
            continue
        score = _link_score(url, text, base_host)
        if score < 1.0:
            continue
        seen.add(url)
        candidates.append(SectionCandidate(title=text, url=url, score=score))

    candidates.sort(key=lambda x: x.score, reverse=True)
    return candidates[:limit]


def _anchor_pattern(anchor: Tag) -> str:
    parts: list[str] = []
    cur: Tag | None = anchor
    depth = 0
    while cur and cur.name and depth < 3:
        ident = cur.name
        classes = cur.get("class") or []
        if classes:
            ident += "." + ".".join(str(c) for c in classes[:2])
        parts.append(ident)
        parent = cur.parent
        cur = parent if isinstance(parent, Tag) else None
        depth += 1
    parts.reverse()
    if parts and parts[-1] == "a":
        parts[-1] = "a[href]"
    return " ".join(parts)


VNECONOMY_LINK_CSS = (
    "div.title-block h2 a[href$='.htm'], "
    "div.item_content h3.name-item a[href$='.htm'], "
    "div.general-item_content h3.name a[href$='.htm']"
)


def suggest_link_css(html: str, base_url: str, *, sample_size: int = 80) -> tuple[str, list[str]]:
    host = (urlparse(base_url).hostname or "").lower()
    if host.endswith("vneconomy.vn"):
        soup = BeautifulSoup(html, "html.parser")
        urls: list[str] = []
        for anchor in soup.select(VNECONOMY_LINK_CSS):
            href = compact_text(anchor.get("href"))
            text = compact_text(anchor.get_text(" "))
            url = normalize_url(urljoin(base_url, href))
            if url and len(text) >= 20:
                urls.append(url)
        if urls:
            return VNECONOMY_LINK_CSS, urls[:sample_size]

    soup = BeautifulSoup(html, "html.parser")
    base_host = (urlparse(base_url).hostname or "").lower().removeprefix("www.")
    patterns: Counter[str] = Counter()
    urls: list[str] = []

    for anchor in soup.find_all("a", href=True):
        href = compact_text(anchor["href"])
        text = compact_text(anchor.get_text(" "))
        url = normalize_url(urljoin(base_url, href))
        if not url:
            continue
        if _link_score(url, text, base_host) < 2.0:
            continue
        patterns[_anchor_pattern(anchor)] += 1
        if len(urls) < sample_size:
            urls.append(url)

    if not patterns:
        return "article a[href], h3 a[href]", urls
    best = patterns.most_common(1)[0][0]
    return best, urls


def suggest_detail_selectors(html: str) -> dict[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    heuristic = extract_article_heuristic(html)
    out: dict[str, str] = {}

    for field_name, css_key in (
        ("title", "title_css"),
        ("summary", "summary_css"),
        ("body", "body_css"),
        ("published_at", "published_at_css"),
    ):
        method = heuristic.methods.get(field_name.replace("_text", ""), "")
        if method:
            out[css_key] = method

    title_node = soup.select_one("h1")
    if title_node and "title_css" not in out:
        classes = title_node.get("class") or []
        out["title_css"] = "h1." + classes[0] if classes else "h1"

    body_node = None
    for selector in ("article.fck_detail", "article", "div#mainContent", "div#contentdetail"):
        node = soup.select_one(selector)
        if node and len(node.find_all("p")) >= 2:
            body_node = node
            out["body_css"] = f"{selector} p"
            break

    if heuristic.summary and "summary_css" not in out:
        out["summary_css"] = "p.description, h2.sapo, .sapo"

    if heuristic.published_at and "published_at_css" not in out:
        out["published_at_css"] = "time[datetime], span.date, span.pdate"

    return out


def discover_from_url(
    url: str,
    *,
    cfg: NewsIngestionConfig | None = None,
    probe_detail: bool = True,
) -> DiscoveryReport:
    cfg = cfg or NewsIngestionConfig()
    report = DiscoveryReport(input_url=url)
    try:
        list_html = _fetch_html(url, cfg)
    except Exception as ex:
        report.notes.append(f"fetch_failed: {ex}")
        return report

    report.sections = discover_sections(list_html, url)
    link_css, sample_urls = suggest_link_css(list_html, url)
    report.suggested_link_css = link_css
    report.sample_article_urls = sample_urls[:10]

    if probe_detail and sample_urls:
        try:
            detail_html = _fetch_html(sample_urls[0], cfg)
            report.suggested_detail = suggest_detail_selectors(detail_html)
            heuristic = extract_article_heuristic(detail_html)
            if not heuristic.title and not heuristic.body_text:
                report.notes.append("detail_heuristic_empty: page may be JS-rendered or blocked")
        except Exception as ex:
            report.notes.append(f"detail_probe_failed: {ex}")

    if not report.sections:
        report.notes.append("no_sections_found: try a category/list URL instead of homepage")
    return report


def report_to_yaml_snippet(report: DiscoveryReport, source_label: str) -> str:
    lines = [
        f"  - id: {source_label}_auto",
        "    enabled: false  # review selectors before enabling",
        f'    list_url: "{report.input_url}"',
        f'    link_css: "{report.suggested_link_css}"',
        f'    source_label: "{source_label}"',
        "    detail:",
    ]
    detail = report.suggested_detail or {
        "title_css": "h1",
        "summary_css": "p.description, h2.sapo",
        "body_css": "article p",
        "published_at_css": "time[datetime], span.date",
    }
    for key, value in detail.items():
        lines.append(f'      {key}: "{value}"')
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Discover HTML news list sections and suggest CSS selectors."
    )
    parser.add_argument("--url", required=True, help="List/category page URL")
    parser.add_argument(
        "--format",
        choices=("json", "yaml", "text"),
        default="text",
        help="Output format",
    )
    parser.add_argument(
        "--source-label",
        default="",
        help="Label for yaml snippet (default: hostname)",
    )
    parser.add_argument("--no-detail", action="store_true", help="Skip detail page probe")
    args = parser.parse_args(argv)

    configure_logging()
    cfg = NewsIngestionConfig()
    report = discover_from_url(
        args.url,
        cfg=cfg,
        probe_detail=not args.no_detail,
    )

    if args.format == "json":
        payload = asdict(report)
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    label = args.source_label or (urlparse(args.url).hostname or "site").split(".")[0]
    if args.format == "yaml":
        print(report_to_yaml_snippet(report, label))
        return 0

    print(f"URL: {report.input_url}")
    print(f"Suggested link_css: {report.suggested_link_css}")
    if report.suggested_detail:
        print("Suggested detail selectors:")
        for key, value in report.suggested_detail.items():
            print(f"  {key}: {value}")
    if report.sections:
        print("\nTop sections:")
        for item in report.sections[:10]:
            print(f"  [{item.score:.1f}] {item.title} -> {item.url}")
    if report.sample_article_urls:
        print("\nSample article URLs:")
        for sample in report.sample_article_urls[:5]:
            print(f"  {sample}")
    if report.notes:
        print("\nNotes:")
        for note in report.notes:
            print(f"  - {note}")
    print("\nYAML snippet:")
    print(report_to_yaml_snippet(report, label))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
