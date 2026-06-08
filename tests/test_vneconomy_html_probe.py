"""Live probe for VnEconomy HTML ingestion (requires network)."""

from __future__ import annotations

import os

import pytest

from ingestion.unstructured_data.config import NewsIngestionConfig
from ingestion.unstructured_data.html_list_adapter import fetch_html_list_news

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_NETWORK_TESTS") != "1",
    reason="Set RUN_NETWORK_TESTS=1 to run live HTML crawl tests",
)

VNECONOMY_LINK_CSS = (
    "div.title-block h2 a[href$='.htm'], "
    "div.item_content h3.name-item a[href$='.htm'], "
    "div.general-item_content h3.name a[href$='.htm']"
)

VNECONOMY_DETAIL = {
    "title_css": "h1",
    "body_css": "main.main-page p",
}


def _spec(source_id: str, list_url: str, section: str) -> dict:
    return {
        "id": source_id,
        "enabled": True,
        "list_url": list_url,
        "link_css": VNECONOMY_LINK_CSS,
        "source_label": "vneconomy",
        "section": section,
        "detail_mode": "hybrid",
        "detail": VNECONOMY_DETAIL,
    }


def test_vneconomy_html_live_probe():
    specs = [
        _spec("vneconomy_thi_truong", "https://vneconomy.vn/thi-truong.htm", "thi-truong"),
        _spec("vneconomy_kinh_te_so", "https://vneconomy.vn/kinh-te-so.htm", "kinh-te-so"),
        _spec("vneconomy_tai_chinh", "https://vneconomy.vn/tai-chinh.htm", "tai-chinh"),
    ]
    cfg = NewsIngestionConfig(
        rate_limit_rpm=120,
        html_max_per_source=5,
        enable_ticker_match=False,
        days_back_html=30,
        strict_published_at_days_back=False,
    )
    df = fetch_html_list_news(cfg, specs)
    assert not df.empty, "expected at least one VnEconomy article"
    assert (df["body_text"].astype(str).str.len() > 100).any(), "expected non-empty body"
    assert df["url"].str.startswith("https://vneconomy.vn/").all()
