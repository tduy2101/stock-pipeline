from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .bronze_reader import SilverWriteResult, write_single_part_parquet
from .config import SilverConfig
from .runs_log import write_runs_entry
from .text_utils import (
    coerce_bool_series,
    has_text,
    lower_nullable,
    normalize_text,
    normalize_text_series,
    upper_nullable,
)

LOGGER = logging.getLogger(__name__)

BCTC_OUTPUT_COLUMNS = [
    "doc_id",
    "source",
    "ticker",
    "year",
    "period_key",
    "title",
    "normalized_title",
    "published_at",
    "url_pdf",
    "url_detail",
    "pdf_path",
    "file_size",
    "sha256",
    "pdf_valid_header",
    "qc_pass",
    "status",
    "error",
    "doc_class",
    "language",
    "is_consolidated",
    "is_explanation",
    "is_disclosure",
    "canonical_priority",
    "keep_for_parse",
    "display_status",
    "is_available_for_web",
    "run_partition",
    "source_file",
    "silver_loaded_at",
]

BCTC_INPUT_COLUMNS = [
    "doc_id",
    "source",
    "ticker",
    "title",
    "published_at",
    "url_pdf",
    "url_detail",
    "year",
    "pdf_path",
    "file_size",
    "sha256",
    "pdf_valid_header",
    "qc_pass",
    "status",
    "error",
    "normalized_title",
    "doc_class",
    "language",
    "is_consolidated",
    "is_explanation",
    "is_disclosure",
    "canonical_priority",
    "keep_for_parse",
    "period_key",
]


@dataclass(slots=True)
class BctcPdfMetaTransformResult:
    dataframe: pd.DataFrame
    input_rows: int
    input_files: list[Path] = field(default_factory=list)
    dq_warnings: list[str] = field(default_factory=list)


def _empty_bctc_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "doc_id": pd.Series(dtype="string"),
            "source": pd.Series(dtype="string"),
            "ticker": pd.Series(dtype="string"),
            "year": pd.Series(dtype="Int64"),
            "period_key": pd.Series(dtype="string"),
            "title": pd.Series(dtype="string"),
            "normalized_title": pd.Series(dtype="string"),
            "published_at": pd.Series(dtype="datetime64[ns, UTC]"),
            "url_pdf": pd.Series(dtype="string"),
            "url_detail": pd.Series(dtype="string"),
            "pdf_path": pd.Series(dtype="string"),
            "file_size": pd.Series(dtype="Int64"),
            "sha256": pd.Series(dtype="string"),
            "pdf_valid_header": pd.Series(dtype="bool"),
            "qc_pass": pd.Series(dtype="bool"),
            "status": pd.Series(dtype="string"),
            "error": pd.Series(dtype="string"),
            "doc_class": pd.Series(dtype="string"),
            "language": pd.Series(dtype="string"),
            "is_consolidated": pd.Series(dtype="bool"),
            "is_explanation": pd.Series(dtype="bool"),
            "is_disclosure": pd.Series(dtype="bool"),
            "canonical_priority": pd.Series(dtype="Int64"),
            "keep_for_parse": pd.Series(dtype="bool"),
            "display_status": pd.Series(dtype="string"),
            "is_available_for_web": pd.Series(dtype="bool"),
            "run_partition": pd.Series(dtype="string"),
            "source_file": pd.Series(dtype="string"),
            "silver_loaded_at": pd.Series(dtype="datetime64[ns, UTC]"),
        }
    )[BCTC_OUTPUT_COLUMNS]


def _read_bctc_input(
    config: SilverConfig,
    *,
    run_partition: str,
    source: str,
) -> tuple[pd.DataFrame, list[Path], int]:
    path = config.bctc_pdf_meta_bronze_path(run_partition, source=source)
    if not path.is_file():
        raise FileNotFoundError(f"Bronze BCTC PDF metadata not found: {path}")
    df = pd.read_parquet(path).copy()
    input_rows = len(df)
    for col in BCTC_INPUT_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    df = df[BCTC_INPUT_COLUMNS]
    df["source_file"] = str(path)
    df["run_partition"] = run_partition
    return df, [path], input_rows


def _infer_period_key(row: pd.Series) -> str:
    raw_period = normalize_text(row.get("period_key"))
    if not pd.isna(raw_period):
        return str(raw_period).strip().upper()

    text = " ".join(
        str(part)
        for part in (row.get("title"), row.get("normalized_title"), row.get("url_pdf"))
        if not pd.isna(part)
    ).lower()

    if re.search(r"\bquý\s*i\b|\bq1\b", text):
        return "Q1"
    if re.search(r"\bquý\s*ii\b|\bq2\b", text):
        return "Q2"
    if re.search(r"\bquý\s*iii\b|\bq3\b", text):
        return "Q3"
    if re.search(r"\bquý\s*iv\b|\bq4\b", text):
        return "Q4"
    if "bán niên" in text or "6 tháng" in text:
        return "H1"
    if "9 tháng" in text:
        return "9M"
    if "năm" in text or "annual" in text or "thường niên" in text:
        return "ANNUAL"
    return "GENERAL"


def _display_status(row: pd.Series) -> str:
    status = "" if pd.isna(row.get("status")) else str(row.get("status")).strip().lower()
    qc_pass = bool(row.get("qc_pass"))
    file_size = row.get("file_size")
    file_size_ok = pd.notna(file_size) and int(file_size) > 0
    has_location = has_text(row.get("pdf_path")) or has_text(row.get("url_pdf"))

    if status == "downloaded" and qc_pass and file_size_ok and has_location:
        return "available"
    if status.startswith("skipped"):
        return "skipped"
    if "failed" in status or not qc_pass:
        return "failed"
    return "unknown"


def transform_bctc_pdf_meta(
    config: SilverConfig,
    *,
    run_partition: str,
    source: str = "hnx",
) -> BctcPdfMetaTransformResult:
    raw, files, input_rows = _read_bctc_input(
        config,
        run_partition=run_partition,
        source=source,
    )
    warnings: list[str] = []
    loaded_at = pd.Timestamp(datetime.now(timezone.utc))
    if raw.empty:
        return BctcPdfMetaTransformResult(
            _empty_bctc_frame(),
            input_rows=0,
            input_files=files,
            dq_warnings=warnings,
        )

    out = raw.copy()
    out["doc_id"] = out["doc_id"].map(normalize_text).astype("string")
    invalid_doc = out["doc_id"].isna() | out["doc_id"].str.strip().eq("")
    if invalid_doc.any():
        warnings.append(f"dropped {int(invalid_doc.sum())} rows with invalid doc_id")
        out = out.loc[~invalid_doc].copy()

    out["source"] = out["source"].map(lambda value: lower_nullable(value, default=source)).astype("string")
    out["ticker"] = out["ticker"].map(upper_nullable).astype("string")
    out["year"] = pd.to_numeric(out["year"], errors="coerce").astype("Int64")
    out["published_at"] = pd.to_datetime(
        out["published_at"],
        errors="coerce",
        utc=True,
        dayfirst=True,
    )
    out["file_size"] = pd.to_numeric(out["file_size"], errors="coerce").astype("Int64")
    out["canonical_priority"] = pd.to_numeric(
        out["canonical_priority"],
        errors="coerce",
    ).astype("Int64")

    for col in ("title", "normalized_title", "error"):
        out[col] = normalize_text_series(out[col])
    for col in ("url_pdf", "url_detail", "pdf_path", "sha256", "status", "doc_class", "language", "source_file", "run_partition"):
        out[col] = out[col].map(normalize_text).astype("string")

    for col in (
        "pdf_valid_header",
        "qc_pass",
        "is_consolidated",
        "is_explanation",
        "is_disclosure",
        "keep_for_parse",
    ):
        out[col] = coerce_bool_series(out[col], default=False)

    duplicate_count = int(out.duplicated(["doc_id"]).sum())
    if duplicate_count:
        warnings.append(f"dropped {duplicate_count} duplicate doc_id rows")
    out["_downloaded_priority"] = out["status"].str.lower().eq("downloaded").astype(int)
    out["_qc_priority"] = out["qc_pass"].astype(int)
    out["_file_size_priority"] = out["file_size"].fillna(0).astype("int64")
    out = out.sort_values(
        ["doc_id", "_downloaded_priority", "_qc_priority", "_file_size_priority"],
        ascending=[True, False, False, False],
        kind="stable",
    )
    out = out.drop_duplicates(["doc_id"], keep="first").copy()

    out["period_key"] = out.apply(_infer_period_key, axis=1).astype("string")
    out["display_status"] = out.apply(_display_status, axis=1).astype("string")
    has_location = out["pdf_path"].map(has_text) | out["url_pdf"].map(has_text)
    out["is_available_for_web"] = (out["display_status"].eq("available") & has_location).astype("bool")
    out["run_partition"] = run_partition
    out["silver_loaded_at"] = loaded_at

    out = out[BCTC_OUTPUT_COLUMNS].sort_values("doc_id", kind="stable").reset_index(drop=True)
    LOGGER.info(
        "Transform bctc_pdf_meta: input_rows=%s output_rows=%s warnings=%s",
        input_rows,
        len(out),
        len(warnings),
    )
    return BctcPdfMetaTransformResult(
        dataframe=out,
        input_rows=input_rows,
        input_files=files,
        dq_warnings=warnings,
    )


def run_bctc_pdf_meta_silver(
    cfg: SilverConfig | None = None,
    *,
    run_partition: str,
    source: str = "hnx",
) -> SilverWriteResult:
    config = cfg or SilverConfig()
    started_at = datetime.now(timezone.utc)
    try:
        transformed = transform_bctc_pdf_meta(
            config,
            run_partition=run_partition,
            source=source,
        )
        for warning in transformed.dq_warnings:
            LOGGER.warning("bctc_pdf_meta DQ warning: %s", warning)
        output_path = write_single_part_parquet(
            transformed.dataframe,
            config.silver_date_partition_dir("bctc_pdf_meta", run_partition),
            cfg=config,
        )
        result = SilverWriteResult(
            dataset="bctc_pdf_meta",
            output_path=output_path,
            input_rows=transformed.input_rows,
            output_rows=len(transformed.dataframe),
            input_files=len(transformed.input_files),
            run_partition=run_partition,
            dq_errors=[],
            dq_warnings=transformed.dq_warnings,
            output_paths=[output_path],
            trading_date_partitions=[],
        )
        write_runs_entry(
            dataset="bctc_pdf_meta",
            silver_root=config.silver_root,
            run_partition=run_partition,
            started_at=started_at,
            input_rows=result.input_rows,
            output_rows=result.output_rows,
            output_path=config.silver_dataset_dir("bctc_pdf_meta"),
            latest_key=run_partition,
            status="success",
        )
        return result
    except Exception as exc:
        write_runs_entry(
            dataset="bctc_pdf_meta",
            silver_root=config.silver_root,
            run_partition=run_partition,
            started_at=started_at,
            input_rows=0,
            output_rows=0,
            output_path=config.silver_dataset_dir("bctc_pdf_meta"),
            status="error",
            error=str(exc),
        )
        raise
