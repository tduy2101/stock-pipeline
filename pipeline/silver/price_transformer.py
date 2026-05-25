from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from ingestion.structure_data.common import (
    max_trading_date_from_partition_dir,
    parse_date_text,
    read_gold_trading_date_watermark,
)

from .bronze_reader import (
    BronzeBatch,
    SilverWriteResult,
    read_partitioned_parquet,
    write_single_part_parquet,
)
from .config import SilverConfig
from .runs_log import write_runs_entry

LOGGER = logging.getLogger(__name__)

PRICE_REQUIRED_COLUMNS = {"ticker", "open", "high", "low", "close", "volume"}


@dataclass(slots=True)
class TransformResult:
    dataframe: pd.DataFrame
    dq_errors: list[str] = field(default_factory=list)
    dq_warnings: list[str] = field(default_factory=list)


def _coerce_bool(series: pd.Series, default: bool = False) -> pd.Series:
    if series.empty:
        return series.astype("boolean")
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(default).astype("boolean")
    truthy = {"1", "true", "t", "yes", "y"}
    falsy = {"0", "false", "f", "no", "n"}

    def _one(value: object) -> bool:
        if pd.isna(value):
            return default
        text = str(value).strip().lower()
        if text in truthy:
            return True
        if text in falsy:
            return False
        return bool(value)

    return series.map(_one).astype("boolean")


def _canonical_ohlcv_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    aliases: dict[str, str] = {}
    date_col: object | None = None
    for col in out.columns:
        key = str(col).strip().lower().replace(" ", "_")
        if key == "trading_date":
            date_col = col
            break
    if date_col is None:
        for col in out.columns:
            key = str(col).strip().lower().replace(" ", "_")
            compact_key = key.replace("_", "")
            if compact_key == "tradingdate":
                date_col = col
                break
    if date_col is None:
        for col in out.columns:
            key = str(col).strip().lower().replace(" ", "_")
            compact_key = key.replace("_", "")
            if compact_key in {"time", "date"}:
                date_col = col
                break

    for col in out.columns:
        key = str(col).strip().lower().replace(" ", "_")
        if col == date_col:
            aliases[col] = "trading_date"
        elif key in {
            "ticker",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "value",
            "source",
            "instrument_type",
            "ingested_at",
            "fetched_at",
            "value_is_derived",
            "is_suspicious",
            "source_file",
            "run_partition",
        }:
            aliases[col] = key
    return out.rename(columns=aliases)


def _coerce_price_like(
    df: pd.DataFrame,
    *,
    dataset: str,
    symbol_column: str,
    expected_instrument_type: str,
) -> TransformResult:
    errors: list[str] = []
    warnings: list[str] = []
    out = _canonical_ohlcv_columns(df)
    missing = sorted(PRICE_REQUIRED_COLUMNS - set(out.columns))
    if missing:
        errors.append(f"missing required columns: {missing}")
        return TransformResult(pd.DataFrame(), errors, warnings)

    out["ticker"] = out["ticker"].astype("string").str.strip().str.upper()
    if "trading_date" not in out.columns:
        errors.append("missing required date column: trading_date/time/tradingDate")
        return TransformResult(pd.DataFrame(), errors, warnings)
    out["trading_date"] = pd.to_datetime(
        out["trading_date"], errors="coerce"
    ).dt.normalize()
    out["fetched_at"] = pd.to_datetime(
        out.get("fetched_at"), errors="coerce", utc=True
    )

    for col in ("open", "high", "low", "close"):
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out["volume"] = pd.to_numeric(out["volume"], errors="coerce").astype("Int64")
    if "value" in out.columns:
        raw_value = pd.to_numeric(out["value"], errors="coerce")
    else:
        raw_value = pd.Series(pd.NA, index=out.index, dtype="Float64")
    derived_value = out["close"] * pd.to_numeric(out["volume"], errors="coerce")
    value_was_missing = raw_value.isna()
    out["value"] = raw_value.where(~value_was_missing, derived_value)
    if "value_is_derived" in out.columns:
        existing_derived = _coerce_bool(out["value_is_derived"], False)
        out["value_is_derived"] = (existing_derived | value_was_missing).astype("boolean")
    else:
        out["value_is_derived"] = value_was_missing.astype("boolean")

    if "is_suspicious" not in out.columns:
        out["is_suspicious"] = False
    out["is_suspicious"] = _coerce_bool(out["is_suspicious"], False)

    if "source" not in out.columns:
        out["source"] = pd.NA
    out["source"] = out["source"].astype("string").str.strip().str.lower()

    if "instrument_type" not in out.columns:
        out["instrument_type"] = expected_instrument_type
    out["instrument_type"] = (
        out["instrument_type"].astype("string").str.strip().str.lower()
    )
    bad_instrument = out["instrument_type"].ne(expected_instrument_type).sum()
    if bad_instrument:
        warnings.append(
            f"{bad_instrument} rows have instrument_type != {expected_instrument_type}"
        )

    invalid_key = out["ticker"].isna() | out["ticker"].eq("") | out["trading_date"].isna()
    if invalid_key.any():
        warnings.append(f"dropped {int(invalid_key.sum())} rows with invalid key")
        out = out.loc[~invalid_key].copy()

    null_close = out["close"].isna().sum()
    if null_close:
        warnings.append(f"{int(null_close)} rows have null close")

    negative_prices = (out[["open", "high", "low", "close"]] < 0).any(axis=1)
    if negative_prices.any():
        warnings.append(f"{int(negative_prices.sum())} rows have negative OHLC values")
        out.loc[negative_prices, "is_suspicious"] = True

    high_low_bad = out["high"].notna() & out["low"].notna() & (out["high"] < out["low"])
    if high_low_bad.any():
        warnings.append(f"{int(high_low_bad.sum())} rows have high < low")
        out.loc[high_low_bad, "is_suspicious"] = True

    duplicate_count = int(out.duplicated(["ticker", "trading_date"]).sum())
    if duplicate_count:
        warnings.append(f"dropped {duplicate_count} duplicate key rows")
        sort_cols = [c for c in ("fetched_at", "source_file") if c in out.columns]
        if sort_cols:
            out = out.sort_values(sort_cols, kind="stable")
        out = out.drop_duplicates(["ticker", "trading_date"], keep="last")

    if symbol_column != "ticker":
        out = out.rename(columns={"ticker": symbol_column})

    standard_cols = [
        symbol_column,
        "trading_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "value",
        "value_is_derived",
        "source",
        "instrument_type",
        "fetched_at",
        "is_suspicious",
        "run_partition",
        "source_file",
    ]
    if "ingested_at" in out.columns:
        out = out.rename(columns={"ingested_at": "bronze_ingested_at"})
        standard_cols.insert(-2, "bronze_ingested_at")

    existing = [c for c in standard_cols if c in out.columns]
    out = out[existing].sort_values([symbol_column, "trading_date"], kind="stable")
    out = out.reset_index(drop=True)

    LOGGER.info(
        "Transform %s: input_rows=%s output_rows=%s warnings=%s errors=%s",
        dataset,
        len(df),
        len(out),
        len(warnings),
        len(errors),
    )
    return TransformResult(out, errors, warnings)


def transform_price(batch: BronzeBatch) -> TransformResult:
    return _coerce_price_like(
        batch.dataframe,
        dataset="price",
        symbol_column="ticker",
        expected_instrument_type="stock",
    )


def transform_index_price(batch: BronzeBatch) -> TransformResult:
    return _coerce_price_like(
        batch.dataframe,
        dataset="index_price",
        symbol_column="index_code",
        expected_instrument_type="index",
    )


def _log_dq(dataset: str, errors: list[str], warnings: list[str]) -> None:
    for warning in warnings:
        LOGGER.warning("%s DQ warning: %s", dataset, warning)
    for error in errors:
        LOGGER.error("%s DQ error: %s", dataset, error)


def _resolve_incremental_watermark(
    config: SilverConfig,
    dataset: str,
    *,
    gold_tables: tuple[str, ...] = (),
) -> str | None:
    silver_watermark = max_trading_date_from_partition_dir(
        config.silver_dataset_dir(dataset)
    )
    gold_watermark = (
        read_gold_trading_date_watermark(gold_tables) if gold_tables else None
    )
    candidates = [
        d
        for d in (
            parse_date_text(gold_watermark),
            parse_date_text(silver_watermark),
        )
        if d
    ]
    return max(candidates) if candidates else None


def _write_by_trading_date(
    df: pd.DataFrame,
    config: SilverConfig,
    dataset: str,
) -> tuple[list[Path], list[str]]:
    if df.empty:
        return [], []

    out = df.copy()
    out["trading_date"] = pd.to_datetime(
        out["trading_date"], errors="coerce"
    ).dt.normalize()
    paths: list[Path] = []
    partitions: list[str] = []
    for trading_date, frame in out.groupby("trading_date", sort=True, dropna=True):
        date_text = pd.Timestamp(trading_date).date().isoformat()
        path = write_single_part_parquet(
            frame.reset_index(drop=True),
            config.silver_partition_dir(dataset, date_text),
            cfg=config,
        )
        paths.append(path)
        partitions.append(date_text)
    return paths, partitions


def _result_output_path(
    config: SilverConfig,
    dataset: str,
    output_paths: list[Path],
) -> Path:
    return output_paths[-1] if output_paths else config.silver_dataset_dir(dataset)


def _latest_key(partitions: list[str]) -> str | None:
    return max(partitions) if partitions else None


def _write_run_success(
    config: SilverConfig,
    result: SilverWriteResult,
    started_at: datetime,
    *,
    latest_key: str | None = None,
) -> None:
    write_runs_entry(
        dataset=result.dataset,
        silver_root=config.silver_root,
        run_partition=result.run_partition,
        started_at=started_at,
        input_rows=result.input_rows,
        output_rows=result.output_rows,
        output_path=config.silver_dataset_dir(result.dataset),
        latest_key=latest_key,
        status="success",
    )


def _write_run_error(
    config: SilverConfig,
    dataset: str,
    started_at: datetime,
    *,
    run_partition: str | None,
    error: Exception,
) -> None:
    write_runs_entry(
        dataset=dataset,
        silver_root=config.silver_root,
        run_partition=run_partition,
        started_at=started_at,
        input_rows=0,
        output_rows=0,
        output_path=config.silver_dataset_dir(dataset),
        status="error",
        error=str(error),
    )


def run_price_silver(
    cfg: SilverConfig | None = None,
    *,
    run_partition: str | None = None,
) -> SilverWriteResult:
    config = cfg or SilverConfig()
    started_at = datetime.now(timezone.utc)
    watermark = None
    try:
        if run_partition is None:
            watermark = _resolve_incremental_watermark(
                config,
                "price",
                gold_tables=("gold.fact_price", "gold.mart_stock_daily"),
            )
        batch = read_partitioned_parquet(
            dataset="price",
            category_dir=config.price_bronze_dir(),
            run_partition=run_partition,
            min_partition_exclusive=watermark,
        )
        if batch.dataframe.empty:
            result = SilverWriteResult(
                dataset="price",
                output_path=config.silver_dataset_dir("price"),
                input_rows=0,
                output_rows=0,
                input_files=0,
                run_partition=run_partition or watermark,
                dq_errors=[],
                dq_warnings=[],
                output_paths=[],
                trading_date_partitions=[],
            )
            _write_run_success(config, result, started_at, latest_key=watermark)
            return result
        transformed = transform_price(batch)
        _log_dq("price", transformed.dq_errors, transformed.dq_warnings)
        if transformed.dq_errors:
            raise ValueError(f"price transform failed: {transformed.dq_errors}")
        output_paths, partitions = _write_by_trading_date(
            transformed.dataframe,
            config,
            "price",
        )
        result = SilverWriteResult(
            dataset="price",
            output_path=_result_output_path(config, "price", output_paths),
            input_rows=len(batch.dataframe),
            output_rows=len(transformed.dataframe),
            input_files=len(batch.files),
            run_partition=(
                batch.run_partition or (batch.partitions[-1] if batch.partitions else None)
            ),
            dq_errors=transformed.dq_errors,
            dq_warnings=transformed.dq_warnings,
            output_paths=output_paths,
            trading_date_partitions=partitions,
        )
        _write_run_success(config, result, started_at, latest_key=_latest_key(partitions))
        return result
    except Exception as exc:
        _write_run_error(
            config,
            "price",
            started_at,
            run_partition=run_partition or watermark,
            error=exc,
        )
        raise


def run_index_price_silver(
    cfg: SilverConfig | None = None,
    *,
    run_partition: str | None = None,
) -> SilverWriteResult:
    config = cfg or SilverConfig()
    started_at = datetime.now(timezone.utc)
    watermark = None
    try:
        if run_partition is None:
            watermark = _resolve_incremental_watermark(config, "index_price")
        batch = read_partitioned_parquet(
            dataset="index_price",
            category_dir=config.index_bronze_dir(),
            run_partition=run_partition,
            min_partition_exclusive=watermark,
        )
        if batch.dataframe.empty:
            result = SilverWriteResult(
                dataset="index_price",
                output_path=config.silver_dataset_dir("index_price"),
                input_rows=0,
                output_rows=0,
                input_files=0,
                run_partition=run_partition or watermark,
                dq_errors=[],
                dq_warnings=[],
                output_paths=[],
                trading_date_partitions=[],
            )
            _write_run_success(config, result, started_at, latest_key=watermark)
            return result
        transformed = transform_index_price(batch)
        _log_dq("index_price", transformed.dq_errors, transformed.dq_warnings)
        if transformed.dq_errors:
            raise ValueError(f"index_price transform failed: {transformed.dq_errors}")
        output_paths, partitions = _write_by_trading_date(
            transformed.dataframe,
            config,
            "index_price",
        )
        result = SilverWriteResult(
            dataset="index_price",
            output_path=_result_output_path(config, "index_price", output_paths),
            input_rows=len(batch.dataframe),
            output_rows=len(transformed.dataframe),
            input_files=len(batch.files),
            run_partition=(
                batch.run_partition or (batch.partitions[-1] if batch.partitions else None)
            ),
            dq_errors=transformed.dq_errors,
            dq_warnings=transformed.dq_warnings,
            output_paths=output_paths,
            trading_date_partitions=partitions,
        )
        _write_run_success(config, result, started_at, latest_key=_latest_key(partitions))
        return result
    except Exception as exc:
        _write_run_error(
            config,
            "index_price",
            started_at,
            run_partition=run_partition or watermark,
            error=exc,
        )
        raise


def output_exists(path: Path) -> bool:
    return path.is_file()
