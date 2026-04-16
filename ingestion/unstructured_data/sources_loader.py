from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger(__name__)


def load_news_sources_yaml(path: Path) -> tuple[list[str], list[dict[str, Any]]]:
    """Return ``(rss_feed_urls, html_source_dicts)``. Missing file → empty lists."""
    if not path.is_file():
        LOGGER.debug("sources yaml not found: %s", path)
        return [], []
    try:
        import yaml
    except ImportError:
        LOGGER.warning("PyYAML not installed — cannot load %s", path)
        return [], []
    with path.open(encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    rss = raw.get("rss_feeds") or []
    if not isinstance(rss, list):
        rss = []
    rss_out = [str(u).strip() for u in rss if str(u).strip()]
    html = raw.get("html_sources") or []
    if not isinstance(html, list):
        html = []
    html_out: list[dict[str, Any]] = []
    for item in html:
        if isinstance(item, dict):
            html_out.append(item)
    return rss_out, html_out
