"""Heuristic article field extraction when site-specific CSS selectors break."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup, Tag

from .schema import compact_text, strip_html

LOGGER = logging.getLogger(__name__)

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

_NOISE_CONTAINER_RE = re.compile(
    r"(nav|menu|footer|header|sidebar|comment|related|banner|ads?|social|breadcrumb|"
    r"tag-list|share|widget|popup|subscribe)",
    re.IGNORECASE,
)
_VN_DATE_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4}),?\s*(\d{1,2}):(\d{2})"), "%d/%m/%Y %H:%M"),
    (re.compile(r"(\d{1,2})-(\d{1,2})-(\d{4})\s*-\s*(\d{1,2}):(\d{2})"), "%d-%m-%Y %H:%M"),
    (re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})"), "%d/%m/%Y"),
]


@dataclass
class HeuristicArticle:
    title: str = ""
    summary: str = ""
    body_text: str = ""
    published_at: datetime | None = None
    methods: dict[str, str] = field(default_factory=dict)


def _meta_content(soup: BeautifulSoup, *keys: str) -> str:
    for key in keys:
        node = soup.find("meta", attrs={"property": key}) or soup.find(
            "meta", attrs={"name": key}
        )
        if node and node.get("content"):
            text = compact_text(node["content"])
            if text:
                return text
    return ""


def _iter_json_ld_objects(soup: BeautifulSoup) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = script.string or script.get_text()
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, list):
            out.extend(x for x in payload if isinstance(x, dict))
        elif isinstance(payload, dict):
            if "@graph" in payload and isinstance(payload["@graph"], list):
                out.extend(x for x in payload["@graph"] if isinstance(x, dict))
            else:
                out.append(payload)
    return out


def _article_type(obj: dict[str, Any]) -> bool:
    t = obj.get("@type") or obj.get("type") or ""
    if isinstance(t, list):
        t = " ".join(str(x) for x in t)
    t = str(t).lower()
    return any(k in t for k in ("newsarticle", "article", "blogposting", "report"))


def _extract_from_json_ld(soup: BeautifulSoup) -> HeuristicArticle:
    result = HeuristicArticle()
    for obj in _iter_json_ld_objects(soup):
        if not _article_type(obj):
            continue
        result.title = compact_text(obj.get("headline") or obj.get("name") or result.title)
        result.summary = compact_text(
            obj.get("description") or obj.get("abstract") or result.summary
        )
        body = obj.get("articleBody") or obj.get("text")
        if body:
            result.body_text = strip_html(body)
        raw_date = obj.get("datePublished") or obj.get("dateCreated") or obj.get("dateModified")
        if raw_date and result.published_at is None:
            result.published_at = _parse_datetime_flexible(str(raw_date))
        if result.title or result.body_text:
            result.methods["json_ld"] = "ok"
            return result
    return result


def _extract_from_open_graph(soup: BeautifulSoup) -> HeuristicArticle:
    title = _meta_content(soup, "og:title", "twitter:title")
    summary = _meta_content(soup, "og:description", "twitter:description", "description")
    published_raw = _meta_content(
        soup,
        "article:published_time",
        "og:article:published_time",
        "pubdate",
        "publish-date",
    )
    published_at = _parse_datetime_flexible(published_raw) if published_raw else None
    methods: dict[str, str] = {}
    if title:
        methods["title"] = "open_graph"
    if summary:
        methods["summary"] = "open_graph"
    if published_at:
        methods["published_at"] = "open_graph"
    return HeuristicArticle(
        title=title,
        summary=summary,
        published_at=published_at,
        methods=methods,
    )


def _css_path(node: Tag) -> str:
    parts: list[str] = []
    cur: Tag | None = node
    depth = 0
    while cur and cur.name and depth < 4:
        ident = cur.name
        node_id = cur.get("id")
        classes = cur.get("class") or []
        if node_id:
            ident += f"#{node_id}"
        elif classes:
            ident += "." + ".".join(str(c) for c in classes[:2])
        parts.append(ident)
        cur = cur.parent if isinstance(cur.parent, Tag) else None
        depth += 1
    return " > ".join(reversed(parts))


def _is_noise_container(node: Tag) -> bool:
    blob = " ".join(
        [
            str(node.get("id") or ""),
            " ".join(str(c) for c in (node.get("class") or [])),
            str(node.name or ""),
        ]
    )
    return bool(_NOISE_CONTAINER_RE.search(blob))


def _score_body_container(node: Tag) -> float:
    if _is_noise_container(node):
        return -1.0
    paragraphs = node.find_all("p", recursive=True)
    if len(paragraphs) < 2:
        return -1.0
    texts = [compact_text(p.get_text(" ")) for p in paragraphs]
    texts = [t for t in texts if len(t) >= 40]
    if len(texts) < 2:
        return -1.0
    total_len = sum(len(t) for t in texts)
    link_heavy = sum(1 for p in paragraphs if p.find("a")) / max(len(paragraphs), 1)
    score = total_len + len(texts) * 50 - link_heavy * 200
    if node.name == "article":
        score += 500
    return score


def _extract_body_heuristic(soup: BeautifulSoup) -> tuple[str, str]:
    candidates: list[tuple[float, Tag]] = []
    for node in soup.find_all(["article", "main", "div", "section"]):
        if not isinstance(node, Tag):
            continue
        score = _score_body_container(node)
        if score > 0:
            candidates.append((score, node))
    if not candidates:
        return "", ""
    candidates.sort(key=lambda x: x[0], reverse=True)
    best = candidates[0][1]
    paragraphs = best.find_all("p", recursive=True)
    body = compact_text(" ".join(compact_text(p.get_text(" ")) for p in paragraphs))
    return body, _css_path(best)


def _extract_title_heuristic(soup: BeautifulSoup) -> tuple[str, str]:
    for css in ("h1.title-detail", "h1.title", "article h1", "main h1", "h1"):
        node = soup.select_one(css)
        if node:
            text = compact_text(node.get_text(" "))
            if len(text) >= 8:
                return text, css
    title_tag = soup.find("title")
    if title_tag:
        text = compact_text(title_tag.get_text(" "))
        text = re.sub(r"\s*[-|]\s*.*$", "", text).strip()
        if len(text) >= 8:
            return text, "title"
    return "", ""


def _extract_summary_heuristic(soup: BeautifulSoup) -> tuple[str, str]:
    for css in (
        "p.description",
        "h2.sapo",
        "h2.lead",
        ".sapo",
        ".news_sapo",
        "article header p",
        "meta[name=description]",
    ):
        if css.startswith("meta"):
            text = _meta_content(soup, "description", "og:description")
            if text:
                return text, css
            continue
        node = soup.select_one(css)
        if node:
            text = compact_text(node.get_text(" "))
            if 20 <= len(text) <= 600:
                return text, css
    return "", ""


def _parse_datetime_flexible(raw: str) -> datetime | None:
    text = compact_text(raw)
    if not text:
        return None
    if text.endswith("Z"):
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            pass
    try:
        ts = datetime.fromisoformat(text)
        if ts.tzinfo is None:
            return ts.replace(tzinfo=VN_TZ).astimezone(timezone.utc)
        return ts.astimezone(timezone.utc)
    except ValueError:
        pass
    cleaned = re.sub(r"^[^,]+,\s*", "", text)
    cleaned = re.sub(r"\s*\(GMT\+7\)$", "", cleaned, flags=re.IGNORECASE)
    for pattern, fmt in _VN_DATE_PATTERNS:
        m = pattern.search(cleaned)
        if not m:
            continue
        try:
            if fmt == "%d/%m/%Y %H:%M":
                local = datetime.strptime(m.group(0).replace(",", ""), fmt)
            elif fmt == "%d-%m-%Y %H:%M":
                local = datetime.strptime(m.group(0), fmt)
            else:
                local = datetime.strptime(m.group(0), fmt)
            return local.replace(tzinfo=VN_TZ).astimezone(timezone.utc)
        except ValueError:
            continue
    return None


def _extract_published_at_heuristic(soup: BeautifulSoup) -> tuple[datetime | None, str]:
    for node in soup.find_all("time"):
        raw = node.get("datetime") or node.get_text(" ")
        parsed = _parse_datetime_flexible(str(raw))
        if parsed:
            return parsed, "time[datetime]"
    for css in ("span.date", "span.pdate", "span.time", "p.time", ".date"):
        node = soup.select_one(css)
        if node:
            parsed = _parse_datetime_flexible(node.get_text(" "))
            if parsed:
                return parsed, css
    meta_raw = _meta_content(
        soup,
        "article:published_time",
        "og:article:published_time",
        "pubdate",
    )
    if meta_raw:
        parsed = _parse_datetime_flexible(meta_raw)
        if parsed:
            return parsed, "meta:published"
    return None, ""


def extract_article_heuristic(html: str) -> HeuristicArticle:
    """Best-effort article extraction without site-specific YAML selectors."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    merged = HeuristicArticle()
    for partial in (_extract_from_json_ld(soup), _extract_from_open_graph(soup)):
        if not merged.title and partial.title:
            merged.title = partial.title
            merged.methods["title"] = partial.methods.get("title") or partial.methods.get("json_ld", "heuristic")
        if not merged.summary and partial.summary:
            merged.summary = partial.summary
            merged.methods["summary"] = partial.methods.get("summary", "heuristic")
        if not merged.body_text and partial.body_text:
            merged.body_text = partial.body_text
            merged.methods["body"] = partial.methods.get("json_ld", "heuristic")
        if merged.published_at is None and partial.published_at:
            merged.published_at = partial.published_at
            merged.methods["published_at"] = partial.methods.get("published_at", "heuristic")

    if not merged.title:
        title, method = _extract_title_heuristic(soup)
        merged.title = title
        if title:
            merged.methods["title"] = method
    if not merged.summary:
        summary, method = _extract_summary_heuristic(soup)
        merged.summary = summary
        if summary:
            merged.methods["summary"] = method
    if not merged.body_text:
        body, method = _extract_body_heuristic(soup)
        merged.body_text = body
        if body:
            merged.methods["body"] = method
    if merged.published_at is None:
        published_at, method = _extract_published_at_heuristic(soup)
        merged.published_at = published_at
        if published_at and method:
            merged.methods["published_at"] = method

    return merged
