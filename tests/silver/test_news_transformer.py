from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from pipeline.silver.config import SilverConfig
from pipeline.silver.news_transformer import (
    NEWS_INPUT_COLUMNS,
    transform_news,
    run_news_silver,
)
from pipeline.silver.ticker_match import (
    TICKER_BLOCKLIST,
    find_mentions,
    find_mentions_in_parts,
    fold_for_match,
    infer_ticker_from_texts,
    pick_primary_ticker,
)


@pytest.fixture
def universe() -> frozenset[str]:
    return frozenset(
        {
            "FPT",
            "VNM",
            "HPG",
            "HCM",
            "LAI",
            "NHA",
            "THU",
            "TIN",
            "TOP",
            "TRA",
            "USD",
            "VUA",
        }
    )


def test_fold_for_match_strips_accents():
    assert "THU NHAP THUE" in fold_for_match("thu nhập thuế")


def test_thu_nhap_not_ticker_without_context(universe):
    text = "Ca nhan can khai thu nhap thue truoc han"
    mentions = find_mentions(text, universe)
    assert "THU" not in mentions


def test_context_ma_fpt(universe):
    text = "Co phieu FPT tang manh trong phien sang"
    mentions = find_mentions(text, universe)
    assert mentions == ["FPT"]


def test_uppercase_short_symbol_without_prefix(universe):
    text = "FPT tang tran sau tin loi nhuan ky luc"
    mentions = find_mentions(text, universe)
    assert mentions == ["FPT"]


def test_blocklist_usd_without_context(universe):
    text = "Ty gia USD tang so voi dong Viet Nam"
    mentions = find_mentions(text, universe)
    assert "USD" not in mentions


def test_currency_amount_usd_is_not_ticker(universe):
    text = "Tong tai san dat 194,4 ty USD trong nam 2026"
    mentions = find_mentions(text, universe)
    assert "USD" not in mentions


def test_context_usd_with_co_phieu(universe):
    text = "Co phieu USD co bien dong lon"
    mentions = find_mentions(text, universe)
    assert "USD" in mentions


@pytest.mark.parametrize(
    ("text", "code"),
    [
        ("Co phieu nha ty phu tiep tuc gay chu y", "NHA"),
        ("Uy ban Chung khoan Nha nuoc cong bo quy dinh moi", "NHA"),
        ("Co phieu tra co tuc bang tien mat trong thang nay", "TRA"),
        ("Dung thu 7 ve thanh khoan; tac gia Ha Thu", "THU"),
        ("Gia Lai sap co them du an nang luong moi", "LAI"),
        ("TP HCM de xuat thu phi ha tang", "HCM"),
    ],
)
def test_common_vietnamese_words_are_not_tickers(universe, text, code):
    mentions = find_mentions(text, universe)
    assert code not in mentions


@pytest.mark.parametrize(
    "text",
    [
        "HOSE: HCM tang manh sau ket qua kinh doanh",
        "Ma HCM tang tran trong phien sang",
        "(HCM) cong bo ke hoach mua co phieu quy",
    ],
)
def test_blocklisted_symbols_need_strong_local_context(universe, text):
    mentions = find_mentions(text, universe)
    assert "HCM" in mentions


def test_false_positive_before_real_ticker_does_not_win(universe):
    text = "Co phieu nha ty phu duoc chu y, FPT tang manh ve cuoi phien"
    mentions = find_mentions(text, universe)
    assert mentions == ["FPT"]


def test_mentions_rank_title_above_body(universe):
    mentions = find_mentions_in_parts(
        ["FPT tang manh", "", "Co phieu VNM co thanh khoan cao"],
        universe,
    )
    assert mentions[:2] == ["FPT", "VNM"]


def test_pick_primary_ticker():
    assert pick_primary_ticker(["VNM", "FPT"]) == "VNM"


def test_infer_ticker_from_texts_title_first(universe):
    ticker = infer_ticker_from_texts(
        ["Tin thi truong", "Co phieu VNM tang tot"],
        universe,
    )
    assert ticker == "VNM"


def _bronze_row(
    *,
    article_id: str,
    source: str,
    title: str,
    body_text: str = "",
    published_at: str | None = "2026-05-19T08:00:00Z",
) -> dict:
    return {
        "article_id": article_id,
        "source": source,
        "ticker": None,
        "title": title,
        "summary": "summary",
        "body_text": body_text,
        "url": f"https://example.com/{article_id}",
        "published_at": published_at,
        "fetched_at": "2026-05-19T09:00:00Z",
        "language": "vi",
        "raw_ref": "{}",
    }


def _write_bronze(tmp_path: Path, run_partition: str) -> SilverConfig:
    root = tmp_path / "data-lake"
    rss_dir = (
        root
        / "raw"
        / "Unstructure_Data"
        / "news"
        / "rss"
        / f"date={run_partition}"
    )
    html_dir = (
        root
        / "raw"
        / "Unstructure_Data"
        / "news"
        / "html"
        / f"date={run_partition}"
    )
    rss_dir.mkdir(parents=True)
    html_dir.mkdir(parents=True)

    shared_id = "abc123"
    rss = pd.DataFrame(
        [
            _bronze_row(
                article_id=shared_id,
                source="vnexpress_kinh_doanh_rss",
                title="RSS title only",
                body_text="",
                published_at="2026-05-19T08:00:00Z",
            ),
            _bronze_row(
                article_id="rss_only",
                source="cafef_index_rss",
                title="RSS only article",
            ),
        ]
    )
    html = pd.DataFrame(
        [
            _bronze_row(
                article_id=shared_id,
                source="vnexpress_html",
                title="HTML full title",
                body_text="Long HTML body " * 20,
                published_at=None,
            ),
        ]
    )
    rss.to_parquet(rss_dir / "PART-000.parquet", index=False)
    html.to_parquet(html_dir / "PART-000.parquet", index=False)

    listing_dir = root / "raw" / "Structure_Data" / "listing" / "master"
    listing_dir.mkdir(parents=True)
    pd.DataFrame(
        {
            "symbol": ["FPT", "VNM", "THU", "USD"],
            "security_type": ["stock"] * 4,
        }
    ).to_parquet(listing_dir / "listing.parquet", index=False)

    return SilverConfig(repo_root=tmp_path)


def test_dedupe_prefers_html_body(tmp_path):
    cfg = _write_bronze(tmp_path, "2026-05-20")
    result = transform_news(cfg, run_partition="2026-05-20")
    out = result.dataframe
    shared = out.loc[out["article_id"] == "abc123"]
    assert len(shared) == 1
    row = shared.iloc[0]
    assert row["source"] == "vnexpress_html"
    assert len(str(row["body_text"])) > 50


def test_dedupe_unique_article_id(tmp_path):
    cfg = _write_bronze(tmp_path, "2026-05-20")
    result = transform_news(cfg, run_partition="2026-05-20")
    assert result.dataframe["article_id"].is_unique


def test_run_news_silver_writes_parquet(tmp_path):
    cfg = _write_bronze(tmp_path, "2026-05-20")
    write_result = run_news_silver(cfg, run_partition="2026-05-20")
    assert write_result.output_path.is_file()
    written = pd.read_parquet(write_result.output_path)
    assert list(written.columns) == list(
        pd.read_parquet(write_result.output_path).columns
    )
    assert write_result.output_rows == len(written)


def test_ticker_fpt_in_article(tmp_path):
    cfg = _write_bronze(tmp_path, "2026-05-21")
    rss_dir = cfg.news_bronze_path("rss", "2026-05-21").parent
    html_dir = cfg.news_bronze_path("html", "2026-05-21").parent
    rss_dir.mkdir(parents=True, exist_ok=True)
    html_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            _bronze_row(
                article_id="fpt1",
                source="test_rss",
                title="Co phieu FPT tang 5% trong phien hom nay",
                body_text="",
            ),
        ]
    ).to_parquet(rss_dir / "PART-000.parquet", index=False)
    pd.DataFrame(columns=NEWS_INPUT_COLUMNS).to_parquet(
        html_dir / "PART-000.parquet",
        index=False,
    )

    result = transform_news(cfg, run_partition="2026-05-21")
    row = result.dataframe.loc[result.dataframe["article_id"] == "fpt1"].iloc[0]
    assert str(row["ticker"]) == "FPT"
    assert "FPT" in list(row["ticker_mentions"])
