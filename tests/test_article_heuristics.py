from __future__ import annotations

from ingestion.unstructured_data.article_heuristics import extract_article_heuristic
from ingestion.unstructured_data.html_discovery import discover_sections, suggest_link_css

VNEXPRESS_LIST_HTML = """
<html><body>
<nav><a href="/the-thao">The thao</a></nav>
<article class="item-news"><h3 class="title-news"><a href="/kinh-doanh/bai-1.html">FPT tang truong</a></h3></article>
<article class="item-news"><h3 class="title-news"><a href="/kinh-doanh/bai-2.html">VNM cong bo ket qua</a></h3></article>
</body></html>
"""

VNEXPRESS_DETAIL_HTML = """
<html><head>
<meta property="og:title" content="FPT tang truong manh"/>
<meta property="og:description" content="Tom tat bai viet"/>
<meta property="article:published_time" content="2026-05-19T08:30:00+07:00"/>
<script type="application/ld+json">
{"@type":"NewsArticle","headline":"FPT tang truong manh","datePublished":"2026-05-19T08:30:00+07:00","articleBody":"<p>Doan 1 du dai de nhan dien noi dung bai viet chung khoan.</p><p>Doan 2 co nhac den ma FPT va xu huong thi truong.</p>"}
</script>
</head><body>
<h1 class="title-detail">FPT tang truong manh</h1>
<p class="description">Tom tat bai viet</p>
<article class="fck_detail"><p>Doan 1 du dai de nhan dien noi dung bai viet chung khoan.</p><p>Doan 2 co nhac den ma FPT va xu huong thi truong.</p></article>
<span class="date">Thứ hai, 19/05/2026, 08:30 (GMT+7)</span>
</body></html>
"""


def test_extract_article_heuristic_json_ld_and_meta():
    result = extract_article_heuristic(VNEXPRESS_DETAIL_HTML)
    assert "FPT" in result.title
    assert len(result.body_text) > 80
    assert result.published_at is not None
    assert result.methods


def test_discover_sections_prefers_business_links():
    sections = discover_sections(VNEXPRESS_LIST_HTML, "https://vnexpress.net/kinh-doanh")
    urls = [item.url for item in sections]
    assert any("kinh-doanh" in url for url in urls)
    assert all("the-thao" not in url for url in urls)


def test_suggest_link_css_finds_item_news_pattern():
    css, urls = suggest_link_css(VNEXPRESS_LIST_HTML, "https://vnexpress.net/kinh-doanh")
    assert "title-news" in css or "item-news" in css
    assert len(urls) >= 2
