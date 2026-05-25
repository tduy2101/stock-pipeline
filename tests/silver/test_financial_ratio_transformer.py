from __future__ import annotations

from datetime import date

import pandas as pd

from pipeline.silver.financial_ratio_transformer import (
    FinancialRatioTransformer,
    filter_new_snapshots,
)


def _transformer(tmp_path) -> FinancialRatioTransformer:
    return FinancialRatioTransformer(str(tmp_path))


def _wide_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ticker": [" aaa ", "bbb"],
            "item": [" P/E  ratio ", "ROE"],
            "item_id": ["PE", "ROE"],
            "ingested_at": ["2026-05-18T01:02:03Z", "2026-05-18T01:02:04Z"],
            "data_source": ["vnstock", "vnstock"],
            "ratio_period": ["quarter", "quarter"],
            "2026-Q1": [1.1, 2.2],
            "2025-Q4": [3.3, 4.4],
            "2025-year": [5.5, 6.6],
            "snapshot_date": [date(2026, 5, 18), date(2026, 5, 18)],
            "run_partition": ["2026-05-18", "2026-05-18"],
            "source_file": ["raw/file.parquet", "raw/file.parquet"],
        }
    )


def test_watermark_uses_full_timestamp_not_date_only():
    snap_old = "2026-05-18T090000"
    snap_new = "2026-05-18T171513"

    assert snap_new > snap_old
    assert not (snap_old > snap_new)


def test_incremental_filter_uses_full_timestamp():
    snapshots = pd.DataFrame(
        {
            "snapshot_date": [
                "2026-05-18T090000",
                "2026-05-18T171513",
                "2026-05-17T120000",
            ]
        }
    )

    result = filter_new_snapshots(snapshots, "2026-05-18T090000")

    assert len(result) == 1
    assert result.iloc[0]["snapshot_date"] == "2026-05-18T171513"


def test_melt_shape(tmp_path):
    transformer = _transformer(tmp_path)
    df = _wide_frame()

    melted = transformer._melt(df)

    assert len(melted) == 2 * 3
    assert "period_raw" in melted.columns
    assert "value" in melted.columns
    assert "2026-Q1" not in melted.columns
    assert "2025-Q4" not in melted.columns
    assert "2025-year" not in melted.columns


def test_parse_quarter(tmp_path):
    transformer = _transformer(tmp_path)
    df = pd.DataFrame({"period_raw": ["2026-Q1"], "value": [1], "_period_col_index": [0]})

    parsed = transformer._parse_period(df)

    row = parsed.iloc[0]
    assert row["period"] == "2026-Q1"
    assert row["period_type"] == "quarter"
    assert row["year"] == 2026
    assert row["quarter"] == 1


def test_parse_annual(tmp_path):
    transformer = _transformer(tmp_path)
    df = pd.DataFrame({"period_raw": ["2025-year"], "value": [1], "_period_col_index": [0]})

    parsed = transformer._parse_period(df)

    row = parsed.iloc[0]
    assert row["period"] == "2025"
    assert row["period_type"] == "annual"
    assert row["year"] == 2025
    assert pd.isna(row["quarter"])


def test_suffix_dedup(tmp_path):
    transformer = _transformer(tmp_path)
    df = pd.DataFrame(
        {
            "ticker": ["AAA", "AAA"],
            "item_id": ["PE", "PE"],
            "period_raw": ["2025-Q4", "2025-Q4_1"],
            "value": [1.0, 2.0],
            "_period_col_index": [0, 1],
        }
    )

    parsed = transformer._parse_period(df)

    assert len(parsed) == 1
    assert parsed.loc[0, "period"] == "2025-Q4"
    assert parsed.loc[0, "value"] == 2.0


def test_coerce_value(tmp_path):
    transformer = _transformer(tmp_path)
    df = pd.DataFrame(
        {
            "ticker": [" aaa ", "bbb"],
            "period": ["2026-Q1", "2026-Q1"],
            "period_type": ["quarter", "quarter"],
            "year": [2026, 2026],
            "quarter": [1, 1],
            "item": [" P/E   ratio ", "ROE"],
            "item_id": ["PE", "ROE"],
            "value": ["not-a-number", "3.14"],
            "data_source": ["vnstock", "vnstock"],
            "snapshot_date": [date(2026, 5, 18), date(2026, 5, 18)],
            "ingested_at": ["2026-05-18T01:02:03Z", "bad-date"],
            "run_partition": ["2026-05-18", "2026-05-18"],
            "source_file": ["raw/a.parquet", "raw/b.parquet"],
        }
    )

    coerced = transformer._coerce(df)

    assert pd.isna(coerced.loc[0, "value"])
    assert coerced["value"].dtype == "float64"
    assert coerced.loc[1, "value"] == 3.14
    assert coerced.loc[0, "ticker"] == "AAA"
    assert coerced.loc[0, "item_name"] == "P/E ratio"


def test_dedup_keep_latest(tmp_path):
    transformer = _transformer(tmp_path)
    df = pd.DataFrame(
        {
            "ticker": ["AAA", "AAA"],
            "period": ["2026-Q1", "2026-Q1"],
            "period_type": ["quarter", "quarter"],
            "year": [2026, 2026],
            "quarter": [1, 1],
            "item_code": ["PE", "PE"],
            "item_name": ["P/E", "P/E"],
            "value": [1.0, 2.0],
            "source": ["vnstock", "vnstock"],
            "snapshot_date": ["2026-05-18T090000", "2026-05-18T171513"],
            "fetched_at": pd.to_datetime(
                ["2026-05-18T09:00:00Z", "2026-05-18T17:15:13Z"],
                utc=True,
            ),
            "run_partition": ["2026-05-18T090000", "2026-05-18T171513"],
            "source_file": ["raw/old.parquet", "raw/new.parquet"],
        }
    )

    deduped = transformer._dedup(df)

    assert len(deduped) == 1
    assert deduped.loc[0, "snapshot_date"] == "2026-05-18T171513"
    assert deduped.loc[0, "value"] == 2.0


def test_write_partition_path(tmp_path):
    bronze_dir = (
        tmp_path
        / "raw"
        / "Structure_Data"
        / "financial_ratio"
        / "snapshot_date=2026-05-18T171513"
    )
    bronze_dir.mkdir(parents=True)
    pd.DataFrame(
        {
            "ticker": ["aaa"],
            "item": [" P/E   ratio "],
            "item_id": ["PE"],
            "ingested_at": ["2026-05-18T01:02:03Z"],
            "data_source": ["vnstock"],
            "ratio_period": ["quarter"],
            "2026-Q1": ["10.5"],
            "2025-year": ["12.0"],
        }
    ).to_parquet(bronze_dir / "AAA.parquet", engine="pyarrow", index=False)

    summary = _transformer(tmp_path).run()

    output_file = (
        tmp_path
        / "silver"
        / "financial_ratio"
        / "period_type=quarter"
        / "year=2026"
        / "PART-000.parquet"
    )
    assert output_file.is_file()
    assert summary["watermark"] == "2026-05-18T171513"
    assert "period_type=quarter/year=2026" in summary["partitions"]

    written = pd.read_parquet(output_file)
    assert len(written) == 1
    assert written.loc[0, "ticker"] == "AAA"
    assert written.loc[0, "period"] == "2026-Q1"
    assert written.loc[0, "snapshot_date"] == "2026-05-18T171513"
