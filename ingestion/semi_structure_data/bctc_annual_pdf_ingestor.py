from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from ingestion.common import wait_for_rate_limit

from .common import ensure_dir, is_pdf_file_valid, safe_filename, sha256_file
from .config import SemiStructuredIngestionConfig
from .document_classifier import (
    apply_canonical_selection,
    build_stable_doc_id,
    enrich_document_row,
    should_download_financial_pdf,
)
from .downloader import DownloadResult, download_pdf_to_path
from .http_client import make_session
from .providers.hnx_disclosure_provider import fetch_hnx_annual_bctc_documents

LOGGER = logging.getLogger(__name__)


def _classification_meta_defaults() -> dict[str, Any]:
    return {
        "normalized_title": "",
        "doc_class": "unknown",
        "language": "UNKNOWN",
        "is_consolidated": False,
        "is_explanation": False,
        "is_disclosure": False,
        "canonical_priority": 99,
        "keep_for_parse": False,
    }


def _extract_classification_fields(doc: dict[str, Any]) -> dict[str, Any]:
    defaults = _classification_meta_defaults()
    return {
        "normalized_title": str(doc.get("normalized_title") or defaults["normalized_title"]),
        "doc_class": str(doc.get("doc_class") or defaults["doc_class"]),
        "language": str(doc.get("language") or defaults["language"]),
        "is_consolidated": bool(doc.get("is_consolidated", defaults["is_consolidated"])),
        "is_explanation": bool(doc.get("is_explanation", defaults["is_explanation"])),
        "is_disclosure": bool(doc.get("is_disclosure", defaults["is_disclosure"])),
        "canonical_priority": int(
            doc.get("canonical_priority") or defaults["canonical_priority"]
        ),
        "keep_for_parse": bool(doc.get("keep_for_parse", defaults["keep_for_parse"])),
    }


def _write_parquet(df: pd.DataFrame, out_path: Path) -> None:
    ensure_dir(out_path.parent)
    try:
        df.to_parquet(out_path, engine="pyarrow", index=False)
    except ImportError:
        df.to_parquet(out_path, engine="fastparquet", index=False)


def _save_bootstrap_marker(cfg: SemiStructuredIngestionConfig, source: str, run_date: str) -> Path:
    marker_path = cfg.marker_path_for_source(source)
    ensure_dir(marker_path.parent)
    payload = {"source": source, "run_date": run_date, "updated_at": datetime.utcnow().isoformat()}
    marker_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return marker_path


def _source_needs_bootstrap(cfg: SemiStructuredIngestionConfig, source: str) -> bool:
    marker_path = cfg.marker_path_for_source(source)
    if cfg.full_bootstrap_once_then_incremental and marker_path.is_file():
        return False
    return cfg.bootstrap_full_history_if_missing and not marker_path.is_file()


def _empty_download_result() -> DownloadResult:
    """Result placeholder khi không đi qua downloader (ví dụ url trống)."""
    return DownloadResult()


def _build_pdf_path(
    cfg: SemiStructuredIngestionConfig,
    *,
    source: str,
    run_date: str,
    ticker: str,
    year: str,
    doc_id: str,
) -> Path:
    pdf_dir = (
        cfg.bctc_pdf_root
        / f"source={source}"
        / f"date={run_date}"
        / f"ticker={ticker}"
        / f"year={year}"
    )
    ensure_dir(pdf_dir)
    return pdf_dir / f"{doc_id}.pdf"


def ingest_bctc_annual_pdfs(cfg: SemiStructuredIngestionConfig | None = None) -> dict[str, Any]:
    cfg = cfg or SemiStructuredIngestionConfig()
    run_date = cfg.run_date

    all_docs: list[dict[str, Any]] = []
    active_sources = [s.strip().lower() for s in cfg.sources if str(s).strip()]
    if not active_sources:
        active_sources = ["hnx"]

    for source in active_sources:
        if source == "hnx":
            docs = fetch_hnx_annual_bctc_documents(cfg)
        else:
            LOGGER.warning("Source %s chua duoc ho tro o phase hien tai", source)
            docs = []
        all_docs.extend(docs)

    # Lam moi classification + canonical tren toan bo batch (ngon tu filename _EN_/_VI_, ...).
    for i, d in enumerate(all_docs):
        all_docs[i] = enrich_document_row(dict(d), cfg)
    apply_canonical_selection(all_docs, cfg)

    metadata_rows: list[dict[str, Any]] = []
    crawl_count = len(all_docs)
    download_ok = 0
    download_fail = 0
    download_skipped_filter = 0

    # Shared session cho toan bo stage download: keep-alive + pool ổn định hơn.
    session = make_session()

    def _rate_limit_wait() -> None:
        wait_for_rate_limit(cfg.rate_limit_rpm)

    try:
        for doc in all_docs:
            doc = dict(doc)
            cls = _extract_classification_fields(doc)
            nt = cls["normalized_title"]
            d_class = cls["doc_class"]
            lang = cls["language"]
            is_cons = cls["is_consolidated"]
            is_expl = cls["is_explanation"]
            is_disc = cls["is_disclosure"]
            can_pri = cls["canonical_priority"]
            keep_parse = cls["keep_for_parse"]

            source = str(doc.get("source") or "unknown").strip().lower()
            ticker = safe_filename(str(doc.get("ticker") or "UNKNOWN").upper())
            year = safe_filename(str(doc.get("year") or "UNKNOWN").upper())
            url_pdf = str(doc.get("url_pdf") or "").strip()
            doc_id = build_stable_doc_id(doc)
            LOGGER.info(
                "doc_classify | doc_id=%s ticker=%s normalized_title=%s doc_class=%s "
                "language=%s keep_for_parse=%s canonical_priority=%s",
                doc_id,
                str(doc.get("ticker") or "").upper(),
                nt[:240],
                d_class,
                lang,
                keep_parse,
                can_pri,
            )
            pdf_path = _build_pdf_path(
                cfg,
                source=source,
                run_date=run_date,
                ticker=ticker,
                year=year,
                doc_id=doc_id,
            )

            status = "download_failed"
            error: str | None = ""
            file_size = 0
            file_sha256: str | None = None
            pdf_valid = False
            qc_pass = False

            if not url_pdf:
                dl_result = _empty_download_result()
                dl_result.error_class = "EmptyUrl"
                dl_result.error = "missing_url_pdf"
                error = dl_result.error
                download_fail += 1
                LOGGER.warning("Download skip doc_id=%s reason=empty_url", doc_id)
            else:
                do_download, skip_reason = should_download_financial_pdf(doc, cfg)
                if not do_download:
                    dl_result = _empty_download_result()
                    dl_result.error_class = "IngestFilter"
                    dl_result.error = skip_reason or "ingest_filter"
                    error = skip_reason or "ingest_filter"
                    status = "skipped_ingest_filter"
                    download_skipped_filter += 1
                    LOGGER.info(
                        "ingest_skip | doc_id=%s reason=%s url=%s",
                        doc_id,
                        skip_reason,
                        url_pdf[:120],
                    )
                else:
                    dl_result = download_pdf_to_path(
                        session,
                        url_pdf,
                        pdf_path,
                        cfg,
                        doc_id=doc_id,
                        rate_limit_wait=_rate_limit_wait,
                    )
                    if dl_result.success and pdf_path.is_file():
                        file_size = pdf_path.stat().st_size
                        pdf_valid = is_pdf_file_valid(pdf_path)
                        qc_pass = pdf_valid and file_size >= int(cfg.min_pdf_bytes)
                        if qc_pass:
                            file_sha256 = sha256_file(pdf_path)
                            status = "downloaded"
                            error = None
                            download_ok += 1
                        else:
                            status = "qc_failed"
                            error = (
                                f"invalid_pdf_or_too_small(size={file_size}, "
                                f"min={cfg.min_pdf_bytes})"
                            )
                            LOGGER.warning(
                                "QC fail doc_id=%s source=%s reason=%s", doc_id, source, error
                            )
                            download_fail += 1
                    else:
                        file_size = pdf_path.stat().st_size if pdf_path.is_file() else 0
                        error = dl_result.error or "download_failed"
                        LOGGER.warning(
                            "Download fail doc_id=%s url=%s err_class=%s err=%s",
                            doc_id,
                            url_pdf,
                            dl_result.error_class,
                            error,
                        )
                        download_fail += 1

            metadata_rows.append(
                {
                    # ----- Schema cũ (giữ nguyên thứ tự/field) -----
                    "doc_id": doc_id,
                    "source": source,
                    "ticker": str(doc.get("ticker") or "").upper(),
                    "title": doc.get("title"),
                    "published_at": doc.get("published_at"),
                    "url_pdf": url_pdf,
                    "url_detail": doc.get("url_detail"),
                    "year": doc.get("year"),
                    "ingest_date": run_date,
                    "pdf_path": str(pdf_path),
                    "file_size": file_size,
                    "sha256": file_sha256,
                    "pdf_valid_header": pdf_valid,
                    "qc_pass": qc_pass,
                    "status": status,
                    "error": error or None,
                    # ----- Schema mới (append, default None/0 luôn được emit) -----
                    "http_status": dl_result.http_status,
                    "content_length": dl_result.content_length,
                    "bytes_downloaded": int(dl_result.bytes_downloaded or 0),
                    "download_seconds": float(dl_result.download_seconds or 0.0),
                    "attempts": int(dl_result.attempts or 0),
                    "integrity_ok": bool(dl_result.integrity_ok),
                    "error_class": dl_result.error_class,
                    # ----- Classification (append) -----
                    "normalized_title": cls["normalized_title"],
                    "doc_class": cls["doc_class"],
                    "language": cls["language"],
                    "is_consolidated": cls["is_consolidated"],
                    "is_explanation": cls["is_explanation"],
                    "is_disclosure": cls["is_disclosure"],
                    "canonical_priority": cls["canonical_priority"],
                    "keep_for_parse": cls["keep_for_parse"],
                }
            )
    finally:
        try:
            session.close()
        except Exception:  # noqa: BLE001
            pass

    meta_path = (
        cfg.bctc_pdf_meta_root / "source=hnx" / f"date={run_date}" / "PART-000.parquet"
    )
    metadata_df = pd.DataFrame(metadata_rows)
    _write_parquet(metadata_df, meta_path)

    marker_paths: list[str] = []
    for source in sorted({str(x.get("source") or "").lower() for x in all_docs if x.get("source")}):
        if _source_needs_bootstrap(cfg, source):
            marker_paths.append(str(_save_bootstrap_marker(cfg, source, run_date)))

    out = {
        "run_date": run_date,
        "documents_crawled": crawl_count,
        "download_ok": download_ok,
        "download_fail": download_fail,
        "download_skipped_filter": download_skipped_filter,
        "metadata_parquet": str(meta_path),
        "bootstrap_markers": marker_paths,
    }
    LOGGER.info("BCTC annual ingest done: %s", out)
    return out
