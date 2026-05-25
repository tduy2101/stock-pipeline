from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

import pandas as pd

from .bronze_reader import (
    BronzeBatch,
    SilverWriteResult,
    read_master_parquet,
    resolve_latest_snapshot_path,
    write_single_part_parquet,
)
from .config import SilverConfig
from .price_transformer import run_index_price_silver, run_price_silver
from .runs_log import write_runs_entry

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class StructureTransformResult:
    dataframe: pd.DataFrame
    dq_errors: list[str] = field(default_factory=list)
    dq_warnings: list[str] = field(default_factory=list)


def _compact_text(value: object) -> object:
    if pd.isna(value):
        return pd.NA
    text = " ".join(str(value).replace("\r", " ").replace("\n", " ").split()).strip()
    if text.lower() in {"", "nan", "none", "null", "<na>"}:
        return pd.NA
    return text


def _compact_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_object_dtype(out[col]) or pd.api.types.is_string_dtype(out[col]):
            out[col] = out[col].map(_compact_text).astype("string")
    return out


def _parse_run_token(value: object) -> pd.Timestamp:
    text = "" if pd.isna(value) else str(value).strip()
    if not text:
        return pd.NaT
    parsed = pd.to_datetime(text, errors="coerce", utc=True)
    if pd.notna(parsed):
        return parsed
    for fmt in ("%Y-%m-%dT%H%M%S", "%Y%m%dT%H%M%S"):
        try:
            return pd.Timestamp(datetime.strptime(text, fmt), tz=timezone.utc)
        except ValueError:
            pass
    return pd.NaT


def _parse_run_series(series: pd.Series) -> pd.Series:
    return series.map(_parse_run_token)


def _parse_dayfirst_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", dayfirst=True)


def _log_dq(dataset: str, errors: list[str], warnings: list[str]) -> None:
    for warning in warnings:
        LOGGER.warning("%s DQ warning: %s", dataset, warning)
    for error in errors:
        LOGGER.error("%s DQ error: %s", dataset, error)


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


def transform_listing(batch: BronzeBatch) -> StructureTransformResult:
    errors: list[str] = []
    warnings: list[str] = []
    df = batch.dataframe.copy()

    missing = [c for c in ("symbol",) if c not in df.columns]
    if missing:
        errors.append(f"missing required columns: {missing}")
        return StructureTransformResult(pd.DataFrame(), errors, warnings)

    out = _compact_text_columns(df)
    if "organ_name" not in out.columns and "organ_name_x" in out.columns:
        out = out.rename(columns={"organ_name_x": "organ_name"})
    if "organ_name_y" in out.columns:
        out = out.drop(columns=["organ_name_y"])
    out["symbol"] = out["symbol"].astype("string").str.strip().str.upper()
    if "exchange" not in out.columns:
        out["exchange"] = "UNKNOWN"
    out["exchange"] = out["exchange"].astype("string").str.strip().str.upper()
    out.loc[out["exchange"].isin(["", "NAN", "NONE", "<NA>"]), "exchange"] = "UNKNOWN"
    if "type" in out.columns:
        out = out.rename(columns={"type": "security_type"})
    if "security_type" not in out.columns:
        out["security_type"] = pd.NA
    out["security_type"] = out["security_type"].astype("string").str.strip().str.lower()
    before_filter = len(out)
    if out["security_type"].notna().any():
        out = out.loc[out["security_type"].eq("stock")].copy()
    elif "id" in out.columns:
        id_values = pd.to_numeric(out["id"], errors="coerce")
        out = out.loc[id_values.eq(1)].copy()
        out["security_type"] = "stock"
    if len(out) != before_filter:
        warnings.append(f"filtered listing rows to stocks: {before_filter} -> {len(out)}")
    if "crawled_at" in out.columns:
        out["run_partition"] = out["crawled_at"].astype("string")
        out["crawled_at"] = _parse_run_series(out["crawled_at"])
    else:
        out["crawled_at"] = pd.NaT
        out["run_partition"] = pd.NA

    invalid_symbol = out["symbol"].isna() | out["symbol"].eq("")
    if invalid_symbol.any():
        warnings.append(f"dropped {int(invalid_symbol.sum())} rows with invalid symbol")
        out = out.loc[~invalid_symbol].copy()

    bad_exchange = ~out["exchange"].isin(["HOSE", "HNX", "UPCOM", "UNKNOWN"])
    if bad_exchange.any():
        warnings.append(f"{int(bad_exchange.sum())} rows have unexpected exchange")

    duplicate_count = int(out.duplicated(["symbol"]).sum())
    if duplicate_count:
        warnings.append(f"dropped {duplicate_count} duplicate symbol rows")
        out = out.sort_values(["crawled_at", "source_file"], kind="stable")
        out = out.drop_duplicates(["symbol"], keep="last")

    standard_cols = [
        "symbol",
        "organ_name",
        "en_organ_name",
        "exchange",
        "security_type",
        "source",
        "crawled_at",
        "run_partition",
        "source_file",
    ]
    existing = [c for c in standard_cols if c in out.columns]
    out = out[existing].sort_values("symbol", kind="stable").reset_index(drop=True)
    LOGGER.info(
        "Transform listing: input_rows=%s output_rows=%s warnings=%s errors=%s",
        len(df),
        len(out),
        len(warnings),
        len(errors),
    )
    return StructureTransformResult(out, errors, warnings)


def transform_company(batch: BronzeBatch) -> StructureTransformResult:
    errors: list[str] = []
    warnings: list[str] = []
    df = batch.dataframe.copy()

    if "ticker" not in df.columns and "symbol" not in df.columns:
        errors.append("missing required column: ticker or symbol")
        return StructureTransformResult(pd.DataFrame(), errors, warnings)

    out = _compact_text_columns(df)
    if "ticker" not in out.columns:
        out["ticker"] = out["symbol"]
    out["ticker"] = out["ticker"].astype("string").str.strip().str.upper()
    if "symbol" in out.columns:
        out["symbol"] = out["symbol"].astype("string").str.strip().str.upper()
        mismatch = out["symbol"].notna() & out["ticker"].notna() & out["symbol"].ne(out["ticker"])
        if mismatch.any():
            warnings.append(f"{int(mismatch.sum())} rows have symbol != ticker")

    if "exchange" in out.columns:
        out["exchange"] = out["exchange"].astype("string").str.strip().str.upper()
    else:
        out["exchange"] = pd.NA

    for col in ("founded_date", "listing_date"):
        if col in out.columns:
            out[col] = _parse_dayfirst_series(out[col])
    for col in ("as_of_date", "fetched_at"):
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], errors="coerce", utc=True)
    if "snapshot_date" in out.columns:
        out["run_partition"] = out["snapshot_date"].astype("string")
        out["snapshot_date"] = _parse_run_series(out["snapshot_date"])
    else:
        out["snapshot_date"] = pd.NaT
        out["run_partition"] = pd.NA

    numeric_cols = [
        "charter_capital",
        "number_of_employees",
        "par_value",
        "listing_price",
        "listed_volume",
        "free_float_percentage",
        "free_float",
        "outstanding_shares",
    ]
    for col in numeric_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    invalid_ticker = out["ticker"].isna() | out["ticker"].eq("")
    if invalid_ticker.any():
        warnings.append(f"dropped {int(invalid_ticker.sum())} rows with invalid ticker")
        out = out.loc[~invalid_ticker].copy()

    for col in [c for c in numeric_cols if c in out.columns]:
        negative_count = int((out[col] < 0).sum())
        if negative_count:
            warnings.append(f"{negative_count} rows have negative {col}")

    if "free_float_percentage" in out.columns:
        suspicious_ffp = int((out["free_float_percentage"] > 100).sum())
        if suspicious_ffp:
            warnings.append(
                f"{suspicious_ffp} rows have free_float_percentage > 100; keep but do not trust for Gold"
            )

    duplicate_count = int(out.duplicated(["ticker"]).sum())
    if duplicate_count:
        warnings.append(f"dropped {duplicate_count} duplicate ticker rows")
    sort_cols = [c for c in ("snapshot_date", "fetched_at", "source_file") if c in out.columns]
    if sort_cols:
        out = out.sort_values(sort_cols, kind="stable")
    out = out.drop_duplicates(["ticker"], keep="last")

    preferred_cols = [
        "ticker",
        "symbol",
        "exchange",
        "company_type",
        "business_model",
        "founded_date",
        "listing_date",
        "charter_capital",
        "number_of_employees",
        "par_value",
        "listing_price",
        "listed_volume",
        "outstanding_shares",
        "as_of_date",
        "ceo_name",
        "ceo_position",
        "auditor",
        "address",
        "phone",
        "fax",
        "email",
        "website",
        "branches",
        "history",
        "free_float_percentage",
        "free_float",
        "source",
        "company_method",
        "snapshot_date",
        "fetched_at",
        "ingest_run_id",
        "run_partition",
        "source_file",
    ]
    existing = [c for c in preferred_cols if c in out.columns]
    rest = [c for c in out.columns if c not in existing]
    out = out[existing + rest].sort_values("ticker", kind="stable").reset_index(drop=True)

    LOGGER.info(
        "Transform company: input_rows=%s output_rows=%s warnings=%s errors=%s",
        len(df),
        len(out),
        len(warnings),
        len(errors),
    )
    return StructureTransformResult(out, errors, warnings)


def run_listing_silver(cfg: SilverConfig | None = None) -> SilverWriteResult:
    config = cfg or SilverConfig()
    started_at = datetime.now(timezone.utc)
    try:
        batch = read_master_parquet(dataset="listing", path=config.listing_bronze_path())
        transformed = transform_listing(batch)
        _log_dq("listing", transformed.dq_errors, transformed.dq_warnings)
        if transformed.dq_errors:
            raise ValueError(f"listing transform failed: {transformed.dq_errors}")
        output_path = write_single_part_parquet(
            transformed.dataframe,
            config.silver_current_dir("listing"),
            cfg=config,
        )
        result = SilverWriteResult(
            "listing",
            output_path,
            len(batch.dataframe),
            len(transformed.dataframe),
            len(batch.files),
            "current",
            transformed.dq_errors,
            transformed.dq_warnings,
        )
        _write_run_success(config, result, started_at, latest_key="current")
        return result
    except Exception as exc:
        _write_run_error(
            config,
            "listing",
            started_at,
            run_partition="current",
            error=exc,
        )
        raise


def run_company_silver(cfg: SilverConfig | None = None) -> SilverWriteResult:
    config = cfg or SilverConfig()
    started_at = datetime.now(timezone.utc)
    try:
        batch = read_master_parquet(
            dataset="company",
            path=resolve_latest_snapshot_path(
                config.structure_bronze_root / "company" / "snapshots",
                file_name="company_overview.parquet",
            ),
        )
        transformed = transform_company(batch)
        _log_dq("company", transformed.dq_errors, transformed.dq_warnings)
        if transformed.dq_errors:
            raise ValueError(f"company transform failed: {transformed.dq_errors}")
        output_path = write_single_part_parquet(
            transformed.dataframe,
            config.silver_current_dir("company"),
            cfg=config,
        )
        result = SilverWriteResult(
            "company",
            output_path,
            len(batch.dataframe),
            len(transformed.dataframe),
            len(batch.files),
            "current",
            transformed.dq_errors,
            transformed.dq_warnings,
        )
        _write_run_success(config, result, started_at, latest_key="current")
        return result
    except Exception as exc:
        _write_run_error(
            config,
            "company",
            started_at,
            run_partition="current",
            error=exc,
        )
        raise


def run_structured_silver(
    cfg: SilverConfig | None = None,
    *,
    price_run_partition: str | None = None,
    index_run_partition: str | None = None,
    include_price: bool = True,
    include_index_price: bool = True,
    include_listing: bool = True,
    include_company: bool = True,
) -> dict[str, SilverWriteResult]:
    config = cfg or SilverConfig()
    results: dict[str, SilverWriteResult] = {}
    if include_price:
        results["price"] = run_price_silver(config, run_partition=price_run_partition)
    if include_index_price:
        results["index_price"] = run_index_price_silver(
            config,
            run_partition=index_run_partition,
        )
    if include_listing:
        results["listing"] = run_listing_silver(config)
    if include_company:
        results["company"] = run_company_silver(config)
    return results
