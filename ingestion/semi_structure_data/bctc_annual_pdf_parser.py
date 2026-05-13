from __future__ import annotations

import logging
from pathlib import Path
import re
from typing import Any

import pandas as pd

from .config import SemiStructuredIngestionConfig

LOGGER = logging.getLogger(__name__)
_YEAR_PATTERN = re.compile(r"20\d{2}")


def _read_parquet(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path)


def _write_parquet(df: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(out_path, engine="pyarrow", index=False)
    except ImportError:
        df.to_parquet(out_path, engine="fastparquet", index=False)


def _metadata_has_downloaded_rows(path: Path) -> bool:
    if not path.is_file():
        return False
    try:
        df = _read_parquet(path)
    except Exception:  # noqa: BLE001
        return False
    if df.empty:
        return False
    if "status" in df.columns:
        return bool((df["status"] == "downloaded").any())
    if "qc_pass" in df.columns:
        return bool((df["qc_pass"] == True).any())  # noqa: E712
    return True


def _resolve_latest_metadata_path(cfg: SemiStructuredIngestionConfig) -> Path | None:
    """Uu tien partition run_date neu co ban ghi download duoc; tranh ket parquet rong."""
    run_date_path = cfg.bctc_pdf_meta_root / "source=hnx" / f"date={cfg.run_date}" / "PART-000.parquet"
    if _metadata_has_downloaded_rows(run_date_path):
        return run_date_path

    source_root = cfg.bctc_pdf_meta_root / "source=hnx"
    if not source_root.is_dir():
        return run_date_path if run_date_path.is_file() else None
    partitions = sorted(
        [x for x in source_root.glob("date=*") if x.is_dir()],
        key=lambda p: p.name,
        reverse=True,
    )
    for part in partitions:
        candidate = part / "PART-000.parquet"
        if _metadata_has_downloaded_rows(candidate):
            if candidate != run_date_path:
                LOGGER.info("Metadata run_date=%s khong co ban ghi downloaded; dung %s", cfg.run_date, candidate)
            return candidate
    if run_date_path.is_file():
        return run_date_path
    for part in partitions:
        candidate = part / "PART-000.parquet"
        if candidate.is_file():
            return candidate
    return None


def _extract_text_basic(pdf_path: Path) -> str:
    import pdfplumber

    texts: list[str] = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            if page_text.strip():
                texts.append(page_text)
    return "\n".join(texts)


def _infer_year_from_metadata(row: pd.Series) -> str | None:
    year = row.get("year")
    if year is not None and str(year).strip():
        return str(year).strip()
    title = str(row.get("title") or "")
    url_pdf = str(row.get("url_pdf") or "")
    for value in (title, url_pdf):
        match = _YEAR_PATTERN.search(value)
        if match:
            return match.group(0)
    return None


def parse_bctc_annual_pdfs(cfg: SemiStructuredIngestionConfig | None = None) -> dict[str, Any]:
    cfg = cfg or SemiStructuredIngestionConfig()
    run_date = cfg.run_date

    meta_path = _resolve_latest_metadata_path(cfg)
    if meta_path is None:
        LOGGER.warning("Khong tim thay metadata parquet de parse.")
        return {
            "run_date": run_date,
            "metadata_parquet": None,
            "parsed_ok": 0,
            "parsed_fail": 0,
            "output_parquet": None,
        }

    meta_df = _read_parquet(meta_path)
    if meta_df.empty:
        out_path = cfg.bctc_text_root / "source=hnx" / f"date={run_date}" / "PART-000.parquet"
        _write_parquet(
            pd.DataFrame(
                columns=[
                    "doc_id",
                    "ticker",
                    "year",
                    "text_len",
                    "needs_ocr",
                    "parser_status",
                    "error",
                    "ingest_date",
                ]
            ),
            out_path,
        )
        return {
            "run_date": run_date,
            "metadata_parquet": str(meta_path),
            "parsed_ok": 0,
            "parsed_fail": 0,
            "output_parquet": str(out_path),
        }

    rows: list[dict[str, Any]] = []
    parsed_ok = 0
    parsed_fail = 0

    parse_candidates = meta_df.copy()
    if "status" in parse_candidates.columns:
        parse_candidates = parse_candidates[parse_candidates["status"] == "downloaded"]
    if "qc_pass" in parse_candidates.columns:
        parse_candidates = parse_candidates[parse_candidates["qc_pass"] == True]  # noqa: E712

    for _, row in parse_candidates.iterrows():
        doc_id = row.get("doc_id")
        pdf_path = Path(str(row.get("pdf_path") or ""))
        parser_status = "ok"
        error = None
        text_len = 0
        needs_ocr = True
        try:
            text = _extract_text_basic(pdf_path)
            text_len = len(text)
            needs_ocr = text_len < int(cfg.min_text_chars)
            parsed_ok += 1
        except Exception as ex:  # noqa: BLE001
            parser_status = "failed"
            error = str(ex)
            parsed_fail += 1
        rows.append(
            {
                "doc_id": doc_id,
                "ticker": row.get("ticker"),
                "year": _infer_year_from_metadata(row),
                "text_len": text_len,
                "needs_ocr": needs_ocr,
                "parser_status": parser_status,
                "error": error,
                "ingest_date": run_date,
            }
        )

    out_path = cfg.bctc_text_root / "source=hnx" / f"date={run_date}" / "PART-000.parquet"
    out_df = pd.DataFrame(rows)
    _write_parquet(out_df, out_path)

    out = {
        "run_date": run_date,
        "metadata_parquet": str(meta_path),
        "parsed_ok": parsed_ok,
        "parsed_fail": parsed_fail,
        "output_parquet": str(out_path),
    }
    LOGGER.info("BCTC annual parse done: %s", out)
    return out

