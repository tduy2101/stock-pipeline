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


def test_dataset_config_has_required_keys_and_columns():
    required = {"glob", "table", "key_cols", "columns", "date_cols", "timestamp_cols", "text_cols"}

    assert set(DATASET_CONFIG) == {
        "price",
        "index_price",
        "listing",
        "company",
        "financial_ratio",
        "price_board",
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
    ]


def test_parse_datasets_comma_separated():
    assert parse_datasets("price,index_price") == ["price", "index_price"]


def test_parse_datasets_rejects_unknown():
    with pytest.raises(ValueError, match="Invalid dataset"):
        parse_datasets("price,unknown")
