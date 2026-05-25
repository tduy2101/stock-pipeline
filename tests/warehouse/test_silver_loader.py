from __future__ import annotations

from datetime import date, datetime, timezone

import pandas as pd
import pytest

from warehouse.loader.cli import parse_datasets
from warehouse.loader.silver_loader import DATASET_CONFIG, prepare_dataframe


def _price_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ticker": ["AAA", "BBB"],
            "trading_date": pd.to_datetime(["2026-05-18", "2026-05-19"]),
            "open": [10.0, 20.0],
            "high": [11.0, 21.0],
            "low": [9.0, 19.0],
            "close": [10.5, 20.5],
            "volume": pd.array([1000, 2000], dtype="Int64"),
            "value": pd.array([10500.0, 41000.0], dtype="Float64"),
            "value_is_derived": pd.array([True, False], dtype="boolean"),
            "source": ["kbs", "kbs"],
            "instrument_type": ["stock", "stock"],
            "fetched_at": pd.to_datetime(
                ["2026-05-18T08:00:00Z", "2026-05-19T08:00:00Z"],
                utc=True,
            ),
            "is_suspicious": pd.array([False, False], dtype="boolean"),
            "bronze_ingested_at": ["2026-05-18T135703", "2026-05-19T135703"],
            "run_partition": ["2026-05", "2026-05"],
            "source_file": ["a.parquet", None],
        }
    )


def test_price_trading_date_becomes_python_date():
    df = prepare_dataframe(_price_frame(), "price")

    assert df["trading_date"].iloc[0] == date(2026, 5, 18)


def test_nullable_int_float_bool_become_python_values_or_none():
    frame = _price_frame()
    frame.loc[0, "volume"] = pd.NA
    frame.loc[0, "value"] = pd.NA
    frame.loc[0, "is_suspicious"] = pd.NA

    df = prepare_dataframe(frame, "price")

    assert df["volume"].iloc[0] is None
    assert df["value"].iloc[0] is None
    assert df["is_suspicious"].iloc[0] is None
    assert isinstance(df["volume"].iloc[1], int)
    assert isinstance(df["value"].iloc[1], float)
    assert isinstance(df["is_suspicious"].iloc[1], bool)


def test_text_columns_keep_text_and_none():
    df = prepare_dataframe(_price_frame(), "price")

    assert df["bronze_ingested_at"].iloc[0] == "2026-05-18T135703"
    assert df["run_partition"].iloc[0] == "2026-05"
    assert df["source_file"].iloc[1] is None


def test_duplicate_key_raises():
    frame = pd.concat([_price_frame(), _price_frame().iloc[[0]]], ignore_index=True)

    with pytest.raises(ValueError, match="duplicate key"):
        prepare_dataframe(frame, "price")


def test_null_key_raises_after_conversion():
    frame = _price_frame()
    frame.loc[0, "trading_date"] = pd.NaT

    with pytest.raises(ValueError, match="null key"):
        prepare_dataframe(frame, "price")


def test_missing_optional_columns_are_filled_with_none():
    frame = _price_frame().drop(columns=["value_is_derived", "source_file"])

    df = prepare_dataframe(frame, "price")

    assert "value_is_derived" in df.columns
    assert "source_file" in df.columns
    assert df["value_is_derived"].isna().all()
    assert df["source_file"].isna().all()


def test_financial_ratio_null_value_is_kept():
    frame = pd.DataFrame(
        {
            "ticker": ["AAA", "BBB"],
            "period": ["2026-Q1", "2026-Q1"],
            "period_type": ["quarter", "quarter"],
            "year": pd.array([2026, 2026], dtype="Int64"),
            "quarter": pd.array([1, 1], dtype="Int64"),
            "item_code": ["EPS", "ROE"],
            "item_name": ["EPS", "ROE"],
            "value": [None, 12.5],
            "source": ["kbs", "kbs"],
            "snapshot_date": [pd.Timestamp("2026-05-18"), "2026-05-18T171513"],
            "fetched_at": pd.to_datetime(["2026-05-18T08:00:00Z"] * 2, utc=True),
            "run_partition": ["2026-05", "2026-05"],
            "source_file": ["ratio.parquet", "ratio.parquet"],
        }
    )

    df = prepare_dataframe(frame, "financial_ratio")

    assert df["value"].iloc[0] is None
    assert df["value"].iloc[1] == 12.5
    assert df["year"].iloc[0] == 2026
    assert df["quarter"].iloc[0] == 1
    assert df["snapshot_date"].iloc[0] == "2026-05-18 00:00:00"


def test_company_date_columns_and_snapshot_text():
    frame = pd.DataFrame(
        {
            "ticker": ["AAA"],
            "symbol": ["AAA"],
            "exchange": ["HOSE"],
            "founded_date": pd.to_datetime(["1990-01-01"]),
            "listing_date": pd.to_datetime(["2010-06-15"]),
            "as_of_date": pd.to_datetime(["2026-03-31T00:00:00Z"], utc=True),
            "snapshot_date": [pd.Timestamp("2026-05-18T13:57:03Z")],
            "fetched_at": pd.to_datetime(["2026-05-18T08:00:00Z"], utc=True),
            "run_partition": ["2026-05-18T135703"],
            "source_file": ["company.parquet"],
        }
    )

    df = prepare_dataframe(frame, "company")

    assert df["founded_date"].iloc[0] == date(1990, 1, 1)
    assert df["listing_date"].iloc[0] == date(2010, 6, 15)
    assert df["as_of_date"].iloc[0] == date(2026, 3, 31)
    assert df["snapshot_date"].iloc[0] == "2026-05-18 13:57:03+00:00"


def test_price_board_date_and_timestamp_columns():
    frame = pd.DataFrame(
        {
            "symbol": ["AAA"],
            "trading_date": [date(2026, 5, 18)],
            "snapshot_at": pd.to_datetime(["2026-05-18T07:40:08Z"], utc=True),
            "is_suspicious": [False],
        }
    )

    df = prepare_dataframe(frame, "price_board")

    assert df["trading_date"].iloc[0] == date(2026, 5, 18)
    assert df["snapshot_at"].iloc[0].to_pydatetime() == datetime(
        2026, 5, 18, 7, 40, 8, tzinfo=timezone.utc
    )


def test_news_special_columns_and_dates():
    frame = pd.DataFrame(
        {
            "article_id": ["a1", "a2"],
            "source": ["cafef_html", "vietstock_rss"],
            "ticker": ["FPT", None],
            "ticker_mentions": [["FPT", "VNM"], pd.array(["HPG"], dtype="string")],
            "title": ["Article 1", "Article 2"],
            "summary": ["Summary 1", ""],
            "body_text": ["Full body", None],
            "url": ["https://example.com/a1", "https://example.com/a2"],
            "published_at": pd.to_datetime(
                ["2026-05-19T09:00:00Z", "2026-05-19T10:00:00Z"],
                utc=True,
            ),
            "published_date": [date(2026, 5, 19), "2026-05-19"],
            "fetched_at": pd.to_datetime(["2026-05-19T11:00:00Z"] * 2, utc=True),
            "language": ["vi", "vi"],
            "word_count": pd.array([100, 20], dtype="Int64"),
            "sentiment_score": pd.array([1, 0], dtype="Int64"),
            "sentiment_label": ["positive", "neutral"],
            "sentiment_method": ["keyword_v1", "keyword_v1"],
            "raw_ref": ['{"feed": "cafef"}', {"feed": "vietstock"}],
            "run_partition": ["2026-05-19", "2026-05-19"],
            "source_file": ["news.parquet", "news.parquet"],
            "silver_loaded_at": pd.to_datetime(["2026-05-23T14:30:33Z"] * 2, utc=True),
        }
    )

    df = prepare_dataframe(frame, "news")

    assert df["published_date"].iloc[0] == date(2026, 5, 19)
    assert df["run_partition"].iloc[0] == date(2026, 5, 19)
    assert df["ticker_mentions"].iloc[0] == ["FPT", "VNM"]
    assert df["ticker_mentions"].iloc[1] == ["HPG"]
    assert df["raw_ref"].iloc[0] == {"feed": "cafef"}
    assert df["raw_ref"].iloc[1] == {"feed": "vietstock"}


def test_news_duplicate_article_keeps_latest_partition():
    frame = pd.DataFrame(
        {
            "article_id": ["same", "same"],
            "title": ["old", "new"],
            "url": ["https://example.com/same", "https://example.com/same"],
            "ticker_mentions": [[], []],
            "raw_ref": ["{}", "{}"],
            "published_date": ["2026-05-19", "2026-05-20"],
            "run_partition": ["2026-05-19", "2026-05-20"],
            "silver_loaded_at": pd.to_datetime(
                ["2026-05-23T01:00:00Z", "2026-05-24T01:00:00Z"],
                utc=True,
            ),
        }
    )

    df = prepare_dataframe(frame, "news")

    assert len(df) == 1
    assert df["title"].iloc[0] == "new"
    assert df["run_partition"].iloc[0] == date(2026, 5, 20)


def test_news_duplicate_url_with_different_article_raises():
    frame = pd.DataFrame(
        {
            "article_id": ["a1", "a2"],
            "title": ["one", "two"],
            "url": ["https://example.com/same", "https://example.com/same"],
            "ticker_mentions": [[], []],
            "raw_ref": ["{}", "{}"],
            "run_partition": ["2026-05-19", "2026-05-19"],
        }
    )

    with pytest.raises(ValueError, match="duplicate non-null values: url"):
        prepare_dataframe(frame, "news")


def test_bctc_pdf_meta_dates_and_bool_columns():
    frame = pd.DataFrame(
        {
            "doc_id": ["d1", "d2"],
            "source": ["hnx", "hnx"],
            "ticker": ["AAA", "BBB"],
            "year": pd.array([2026, 2026], dtype="Int64"),
            "period_key": ["Q1", "ANNUAL"],
            "title": ["BCTC Q1", "BCTC annual"],
            "normalized_title": ["bctc q1", "bctc annual"],
            "published_at": pd.to_datetime(
                ["2026-05-14T01:00:00Z", "2026-05-15T01:00:00Z"],
                utc=True,
            ),
            "url_pdf": ["https://example.com/d1.pdf", "https://example.com/d2.pdf"],
            "url_detail": ["https://example.com", "https://example.com"],
            "pdf_path": ["d1.pdf", "d2.pdf"],
            "file_size": pd.array([1000, 0], dtype="Int64"),
            "sha256": ["abc", None],
            "pdf_valid_header": pd.array([True, False], dtype="boolean"),
            "qc_pass": pd.array([True, False], dtype="boolean"),
            "status": ["downloaded", "skipped_ingest_filter"],
            "error": [None, "skipped_doc_class:disclosure"],
            "doc_class": ["financial_statement_separate", "disclosure"],
            "language": ["VI", "VI"],
            "is_consolidated": pd.array([False, False], dtype="boolean"),
            "is_explanation": pd.array([False, False], dtype="boolean"),
            "is_disclosure": pd.array([False, True], dtype="boolean"),
            "canonical_priority": pd.array([2, 99], dtype="Int64"),
            "keep_for_parse": pd.array([True, False], dtype="boolean"),
            "display_status": ["available", "skipped"],
            "is_available_for_web": pd.array([True, False], dtype="boolean"),
            "run_partition": ["2026-05-14", "2026-05-14"],
            "source_file": ["bctc.parquet", "bctc.parquet"],
            "silver_loaded_at": pd.to_datetime(["2026-05-18T08:37:26Z"] * 2, utc=True),
        }
    )

    df = prepare_dataframe(frame, "bctc_pdf_meta")

    assert df["run_partition"].iloc[0] == date(2026, 5, 14)
    assert isinstance(df["is_explanation"].iloc[0], bool)
    assert isinstance(df["is_disclosure"].iloc[1], bool)
    assert isinstance(df["is_available_for_web"].iloc[0], bool)


def test_dataset_config_has_required_keys_and_columns():
    required = {"glob", "table", "key_cols", "columns", "date_cols", "timestamp_cols", "text_cols"}

    assert set(DATASET_CONFIG) == {
        "price",
        "index_price",
        "listing",
        "company",
        "financial_ratio",
        "price_board",
        "news",
        "bctc_pdf_meta",
    }
    for dataset, cfg in DATASET_CONFIG.items():
        assert not (required - set(cfg)), dataset
        for key_col in cfg["key_cols"]:
            assert key_col in cfg["columns"]


def test_parse_datasets_all():
    assert parse_datasets("all") == [
        "price",
        "index_price",
        "listing",
        "company",
        "financial_ratio",
        "price_board",
        "news",
        "bctc_pdf_meta",
    ]


def test_parse_datasets_comma_separated():
    assert parse_datasets("price,index_price") == ["price", "index_price"]


def test_parse_datasets_rejects_unknown():
    with pytest.raises(ValueError, match="Invalid dataset"):
        parse_datasets("price,unknown")
