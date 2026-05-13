from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from hashlib import sha1
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pandas as pd
import requests

from ingestion.common import call_with_retry, wait_for_rate_limit

from .common import ensure_dir, is_pdf_file_valid, safe_filename, sha256_file
from .config import SemiStructuredIngestionConfig
from .providers.hnx_disclosure_provider import fetch_hnx_annual_bctc_documents

LOGGER = logging.getLogger(__name__)

_DEFAULT_REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36 stock-pipeline-bctc/1.0"
    ),
    "Accept": "application/pdf,*/*;q=0.8",
}
_DOWNLOAD_CHUNK_SIZE = 256 * 1024


def _certifi_bundle() -> bool | str:
    try:
        import certifi

        return certifi.where()
    except ImportError:
        return True


def _download_verify_for_url(url: str, cfg: SemiStructuredIngestionConfig) -> bool | str:
    """Mirror SSL behavior from HNX crawler for HNX download hosts."""
    host = (urlparse(url).hostname or "").lower()
    is_hnx_host = host.endswith("hnx.vn")
    if not is_hnx_host:
        return True

    env = os.environ.get("HNX_SSL_VERIFY", "").strip().lower()
    if env in ("0", "false", "no"):
        try:
            import urllib3

            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        except Exception:  # noqa: BLE001
            pass
        return False
    if env in ("1", "true", "yes"):
        return _certifi_bundle()
    if not cfg.hnx_verify_ssl:
        try:
            import urllib3

            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        except Exception:  # noqa: BLE001
            pass
        return False
    return _certifi_bundle()


def _build_doc_id(doc: dict[str, Any]) -> str:
    key = "|".join(
        [
            str(doc.get("source") or ""),
            str(doc.get("ticker") or ""),
            str(doc.get("title") or ""),
            str(doc.get("published_at") or ""),
            str(doc.get("url_pdf") or ""),
        ]
    )
    return sha1(key.encode("utf-8")).hexdigest()


def _download_pdf_bytes(
    url: str,
    cfg: SemiStructuredIngestionConfig,
) -> bytes:
    def _request() -> bytes:
        wait_for_rate_limit(cfg.rate_limit_rpm)
        with requests.get(
            url,
            timeout=cfg.request_timeout_sec,
            headers=_DEFAULT_REQUEST_HEADERS,
            verify=_download_verify_for_url(url, cfg),
            stream=True,
        ) as response:
            response.raise_for_status()
            buf = bytearray()
            for chunk in response.iter_content(chunk_size=_DOWNLOAD_CHUNK_SIZE):
                if chunk:
                    buf.extend(chunk)
            return bytes(buf)

    return call_with_retry(
        _request,
        max_attempts=max(1, int(cfg.download_retry_max_attempts)),
        base_delay_sec=cfg.api_retry_base_delay_sec,
        label=f"download:{url}",
    )


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

    metadata_rows: list[dict[str, Any]] = []
    crawl_count = len(all_docs)
    download_ok = 0
    download_fail = 0

    for doc in all_docs:
        source = str(doc.get("source") or "unknown").strip().lower()
        ticker = safe_filename(str(doc.get("ticker") or "UNKNOWN").upper())
        year = safe_filename(str(doc.get("year") or "UNKNOWN").upper())
        url_pdf = str(doc.get("url_pdf") or "").strip()
        doc_id = _build_doc_id(doc)
        pdf_dir = (
            cfg.bctc_pdf_root
            / f"source={source}"
            / f"date={run_date}"
            / f"ticker={ticker}"
            / f"year={year}"
        )
        ensure_dir(pdf_dir)
        pdf_path = pdf_dir / f"{doc_id}.pdf"

        status = "download_failed"
        error = ""
        file_size = 0
        file_sha256 = None
        pdf_valid = False
        qc_pass = False

        try:
            pdf_content = _download_pdf_bytes(url_pdf, cfg)
            pdf_path.write_bytes(pdf_content)
            file_size = pdf_path.stat().st_size
            pdf_valid = is_pdf_file_valid(pdf_path)
            qc_pass = pdf_valid and file_size >= int(cfg.min_pdf_bytes)
            if not qc_pass:
                status = "qc_failed"
                error = f"invalid_pdf_or_too_small(size={file_size}, min={cfg.min_pdf_bytes})"
                LOGGER.warning("QC fail doc_id=%s source=%s reason=%s", doc_id, source, error)
                download_fail += 1
            else:
                file_sha256 = sha256_file(pdf_path)
                status = "downloaded"
                download_ok += 1
        except Exception as ex:  # noqa: BLE001
            download_fail += 1
            error = str(ex)
            LOGGER.warning("Download fail doc_id=%s url=%s err=%s", doc_id, url_pdf, ex)

        metadata_rows.append(
            {
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
            }
        )

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
        "metadata_parquet": str(meta_path),
        "bootstrap_markers": marker_paths,
    }
    LOGGER.info("BCTC annual ingest done: %s", out)
    return out

