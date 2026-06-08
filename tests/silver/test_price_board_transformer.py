from __future__ import annotations

from datetime import date, datetime

import pandas as pd

from pipeline.silver.price_board_transformer import (
    PriceBoardTransformer,
    dedupe_to_daily_latest,
)


def _transformer(tmp_path) -> PriceBoardTransformer:
    return PriceBoardTransformer(str(tmp_path))


def _bronze_dir(tmp_path, snapshot_at: str):
    path = (
        tmp_path
        / "raw"
        / "Structure_Data"
        / "price_board"
        / f"snapshot_at={snapshot_at}"
    )
    path.mkdir(parents=True)
    return path


def test_parse_snapshot_at(tmp_path):
    transformer = _transformer(tmp_path)

    parsed = transformer._parse_snapshot_at("2026-05-18T15-00-00")

    assert parsed == datetime(2026, 5, 18, 15, 0, 0)


def test_trading_date_from_snapshot(tmp_path):
    transformer = _transformer(tmp_path)
    df = pd.DataFrame({"symbol": ["aaa"], "snapshot_at": [datetime(2026, 5, 18, 15)]})

    coerced = transformer._coerce(df)

    assert coerced.loc[0, "trading_date"] == date(2026, 5, 18)


def test_coerce_price_to_float(tmp_path):
    transformer = _transformer(tmp_path)
    df = pd.DataFrame(
        {
            "symbol": ["aaa", "bbb"],
            "close_price": ["12.5", "abc"],
            "snapshot_at": [datetime(2026, 5, 18, 15), datetime(2026, 5, 18, 15)],
        }
    )

    coerced = transformer._coerce(df)

    assert coerced["close_price"].dtype == "float64"
    assert coerced.loc[0, "close_price"] == 12.5
    assert pd.isna(coerced.loc[1, "close_price"])


def test_coerce_volume_to_int64(tmp_path):
    transformer = _transformer(tmp_path)
    df = pd.DataFrame(
        {
            "symbol": ["aaa", "bbb"],
            "volume_accumulated": ["1000", None],
            "snapshot_at": [datetime(2026, 5, 18, 15), datetime(2026, 5, 18, 15)],
        }
    )

    coerced = transformer._coerce(df)

    assert str(coerced["volume_accumulated"].dtype) == "Int64"
    assert coerced.loc[0, "volume_accumulated"] == 1000
    assert pd.isna(coerced.loc[1, "volume_accumulated"])


def test_coerce_listing_symbol_bronze_schema(tmp_path):
    transformer = _transformer(tmp_path)
    df = pd.DataFrame(
        {
            "listing_symbol": ["acb", "HPG"],
            "listing_exchange": ["HOSE", None],
            "match_match_price": [26100, 42350],
            "match_accumulated_volume": [3033000, 879800],
            "match_reference_price": [26000, 41900],
            "data_source": ["vci", "vci"],
            "snapshot_at": [datetime(2026, 6, 4, 2, 37, 18)] * 2,
        }
    )

    coerced = transformer._coerce(df)

    assert len(coerced) == 2
    assert coerced.loc[0, "symbol"] == "ACB"
    assert coerced.loc[1, "symbol"] == "HPG"
    assert coerced.loc[0, "close_price"] == 26100.0
    assert coerced.loc[0, "volume_accumulated"] == 3033000


def test_exchange_fillna(tmp_path):
    transformer = _transformer(tmp_path)
    df = pd.DataFrame(
        {
            "symbol": ["aaa"],
            "exchange": [None],
            "snapshot_at": [datetime(2026, 5, 18, 15)],
        }
    )

    coerced = transformer._coerce(df)

    assert coerced.loc[0, "exchange"] == "UNKNOWN"


def test_dq_flag_close_zero(tmp_path):
    transformer = _transformer(tmp_path)
    df = pd.DataFrame(
        {
            "symbol": ["aaa"],
            "close_price": [0],
            "snapshot_at": [datetime(2026, 5, 18, 15)],
        }
    )

    flagged = transformer._dq_flags(transformer._coerce(df))

    assert bool(flagged.loc[0, "is_suspicious"]) is True


def test_dq_flag_high_lt_low(tmp_path):
    transformer = _transformer(tmp_path)
    df = pd.DataFrame(
        {
            "symbol": ["aaa"],
            "high_price": [10],
            "low_price": [12],
            "snapshot_at": [datetime(2026, 5, 18, 15)],
        }
    )

    flagged = transformer._dq_flags(transformer._coerce(df))

    assert bool(flagged.loc[0, "is_suspicious"]) is True


def test_dedup_keep_latest_snapshot(tmp_path):
    transformer = _transformer(tmp_path)
    df = pd.DataFrame(
        {
            "symbol": ["aaa", "aaa"],
            "close_price": [10.0, 11.0],
            "snapshot_at": [
                datetime(2026, 5, 18, 10),
                datetime(2026, 5, 18, 15),
            ],
            "source_file": ["raw/old.parquet", "raw/new.parquet"],
        }
    )

    deduped = transformer._dedup(transformer._dq_flags(transformer._coerce(df)))

    assert len(deduped) == 1
    assert deduped.loc[0, "snapshot_at"] == pd.Timestamp(
        "2026-05-18T15:00:00Z"
    )
    assert deduped.loc[0, "close_price"] == 11.0


def test_price_board_keeps_latest_snapshot_per_day():
    df = pd.DataFrame(
        {
            "symbol": ["AAA", "AAA", "BBB"],
            "trading_date": ["2026-05-18", "2026-05-18", "2026-05-18"],
            "snapshot_at": [
                pd.Timestamp("2026-05-18 07:40:00", tz="UTC"),
                pd.Timestamp("2026-05-18 14:30:00", tz="UTC"),
                pd.Timestamp("2026-05-18 07:40:00", tz="UTC"),
            ],
            "close_price": [10.0, 10.5, 20.0],
        }
    )

    result = dedupe_to_daily_latest(df)

    assert len(result) == 2
    aaa = result[result["symbol"] == "AAA"].iloc[0]
    assert aaa["close_price"] == 10.5
    assert aaa["snapshot_at"] == pd.Timestamp("2026-05-18 14:30:00", tz="UTC")


def test_write_partition_path(tmp_path):
    bronze_dir = _bronze_dir(tmp_path, "2026-05-18T15-00-00")
    pd.DataFrame(
        {
            "symbol": [" aaa "],
            "exchange": [" hose "],
            "reference_price": ["10.0"],
            "ceiling_price": ["11.0"],
            "floor_price": ["9.0"],
            "open_price": ["10.1"],
            "high_price": ["10.5"],
            "low_price": ["10.0"],
            "close_price": ["10.4"],
            "average_price": ["10.3"],
            "volume_accumulated": ["1000"],
            "total_value": ["10400"],
            "price_change": ["0.4"],
            "percent_change": ["4.0"],
            "data_source": ["vnstock"],
        }
    ).to_parquet(bronze_dir / "PRICE_BOARD_SNAPSHOT.parquet", engine="pyarrow", index=False)

    summary = _transformer(tmp_path).run()

    output_file = (
        tmp_path
        / "silver"
        / "price_board"
        / "trading_date=2026-05-18"
        / "PART-000.parquet"
    )
    assert output_file.is_file()
    assert summary["watermark"] == "2026-05-18"
    assert summary["partitions"] == ["2026-05-18"]

    written = pd.read_parquet(output_file)
    assert len(written) == 1
    assert written.loc[0, "symbol"] == "AAA"
    assert written.loc[0, "exchange"] == "HOSE"
    assert written.loc[0, "source_file"].endswith(
        "raw/Structure_Data/price_board/snapshot_at=2026-05-18T15-00-00/PRICE_BOARD_SNAPSHOT.parquet"
    )


def test_rerun_same_day_overwrites_with_latest_snapshot(tmp_path):
    existing_dir = (
        tmp_path
        / "silver"
        / "price_board"
        / "trading_date=2026-05-18"
    )
    existing_dir.mkdir(parents=True)
    pd.DataFrame(
        {
            "symbol": ["AAA"],
            "trading_date": [date(2026, 5, 18)],
            "snapshot_at": pd.to_datetime(["2026-05-18T07:40:00Z"], utc=True),
            "close_price": [9.5],
        }
    ).to_parquet(
        existing_dir / "PART-000.parquet",
        engine="pyarrow",
        index=False,
    )

    old_bronze_dir = _bronze_dir(tmp_path, "2026-05-18T07-40-00")
    pd.DataFrame(
        {
            "symbol": ["aaa"],
            "close_price": [10.0],
            "data_source": ["vnstock"],
        }
    ).to_parquet(
        old_bronze_dir / "PRICE_BOARD_SNAPSHOT.parquet",
        engine="pyarrow",
        index=False,
    )
    new_bronze_dir = _bronze_dir(tmp_path, "2026-05-18T15-00-00")
    pd.DataFrame(
        {
            "symbol": ["aaa"],
            "close_price": [11.0],
            "data_source": ["vnstock"],
        }
    ).to_parquet(
        new_bronze_dir / "PRICE_BOARD_SNAPSHOT.parquet",
        engine="pyarrow",
        index=False,
    )

    summary = _transformer(tmp_path).run()

    assert summary["rows_written"] == 1
    assert summary["partitions"] == ["2026-05-18"]
    assert summary["watermark"] == "2026-05-18"

    output_files = sorted(existing_dir.glob("PART-*.parquet"))
    assert output_files == [existing_dir / "PART-000.parquet"]
    written = pd.read_parquet(output_files[0])
    assert len(written) == 1
    assert written.loc[0, "close_price"] == 11.0
    assert written.duplicated(["symbol", "trading_date"]).sum() == 0


def test_price_board_no_duplicate_key():
    import glob

    files = glob.glob("data-lake/silver/price_board/**/*.parquet", recursive=True)
    if not files:
        return
    df = pd.concat([pd.read_parquet(file) for file in files], ignore_index=True)
    dupes = df.duplicated(subset=["symbol", "trading_date"])
    assert dupes.sum() == 0, (
        f"Found {dupes.sum()} duplicate (symbol + trading_date)"
    )
