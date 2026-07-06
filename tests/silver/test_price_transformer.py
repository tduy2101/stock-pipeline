from __future__ import annotations

import pandas as pd

from pipeline.silver.price_transformer import (
    _normalize_trading_value_vnd,
    transform_price,
)
from pipeline.silver.bronze_reader import BronzeBatch


def test_derived_stock_value_uses_thousands_vnd_factor():
    close = pd.Series([62.9], dtype="Float64")
    volume = pd.Series([3_000_000], dtype="Int64")
    raw_value = pd.Series([pd.NA], dtype="Float64")

    value, was_missing = _normalize_trading_value_vnd(
        close,
        volume,
        raw_value,
        instrument_type="stock",
    )

    assert was_missing.iloc[0]
    assert value.iloc[0] == 62.9 * 3_000_000 * 1000


def test_api_stock_value_already_in_vnd_is_kept():
    close = pd.Series([62.9], dtype="Float64")
    volume = pd.Series([3_000_000], dtype="Int64")
    raw_value = pd.Series([62.9 * 3_000_000 * 1000], dtype="Float64")

    value, was_missing = _normalize_trading_value_vnd(
        close,
        volume,
        raw_value,
        instrument_type="stock",
    )

    assert not was_missing.iloc[0]
    assert value.iloc[0] == raw_value.iloc[0]


def test_api_stock_value_matching_close_times_volume_is_scaled():
    close = pd.Series([20.5], dtype="Float64")
    volume = pd.Series([2000], dtype="Int64")
    raw_value = pd.Series([41_000.0], dtype="Float64")

    value, was_missing = _normalize_trading_value_vnd(
        close,
        volume,
        raw_value,
        instrument_type="stock",
    )

    assert not was_missing.iloc[0]
    assert value.iloc[0] == 41_000_000.0


def test_index_value_does_not_apply_thousands_vnd_factor():
    close = pd.Series([1250.5], dtype="Float64")
    volume = pd.Series([100_000], dtype="Int64")
    raw_value = pd.Series([pd.NA], dtype="Float64")

    value, was_missing = _normalize_trading_value_vnd(
        close,
        volume,
        raw_value,
        instrument_type="index",
    )

    assert was_missing.iloc[0]
    assert value.iloc[0] == 1250.5 * 100_000


def test_transform_price_writes_value_in_full_vnd():
    batch = BronzeBatch(
        dataset="price",
        dataframe=pd.DataFrame(
            {
                "ticker": ["VCB"],
                "trading_date": ["2026-07-01"],
                "open": [62.0],
                "high": [63.0],
                "low": [61.5],
                "close": [62.9],
                "volume": [3_000_000],
                "instrument_type": ["stock"],
            }
        ),
        files=[],
        partitions=[],
        run_partition=None,
    )

    result = transform_price(batch)

    assert result.dq_errors == []
    assert result.dataframe.loc[0, "value"] == 62.9 * 3_000_000 * 1000
    assert bool(result.dataframe.loc[0, "value_is_derived"])
