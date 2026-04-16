from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pandas as pd
import requests

from ingestion.common import (
    call_with_retry,
    register_vnstock_api_key_from_env,
    save_partition_parquet,
    wait_for_rate_limit,
)
from ingestion.structure_data.config import IngestionConfig as StructureIngestionConfig
from ingestion.structure_data.stock_info_ingestor import ingest_listing

from .config import BctcPdfConfig
from .vnstock_bctc_discovery import discover_bctc_rows_vnstock

LOGGER = logging.getLogger(__name__)

_MANIFEST_COLS = [
    "symbol",
    "exchange",
    "year",
    "quarter",
    "title",
    "source_url",
    "file_path",
    "file_sha256",
    "downloaded_at",
    "http_status",
    "error",
    "discovery_source",
    "run_date",
]


def _hash_file(path: Path) -> str:
    dig = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            dig.update(chunk)
    return dig.hexdigest()


def _safe_filename_from_url(url: str) -> str:
    path = urlparse(url).path.strip("/")
    name = Path(path).name if path else "document.pdf"
    name = re.sub(r'[<>:"/\\|?*]', "_", name)
    return name[:180] if name else "document.pdf"


def _load_known_sha(manifest_path: Path) -> set[str]:
    if not manifest_path.is_file():
        return set()
    try:
        df = pd.read_parquet(manifest_path)
    except Exception:
        return set()
    if "file_sha256" not in df.columns:
        return set()
    return set(df["file_sha256"].dropna().astype(str).tolist())


def _merge_master_manifest(master_path: Path, new_rows: pd.DataFrame) -> None:
    master_path.parent.mkdir(parents=True, exist_ok=True)
    if new_rows.empty:
        return
    if master_path.is_file():
        old = pd.read_parquet(master_path)
        merged = pd.concat([old, new_rows], ignore_index=True)
    else:
        merged = new_rows
    if "file_sha256" in merged.columns:
        merged = merged.drop_duplicates(subset=["file_sha256"], keep="last")
    merged.to_parquet(master_path, engine="pyarrow", index=False)
    LOGGER.info("master manifest -> %s rows %s", len(merged), master_path)


def _read_listing(cfg: BctcPdfConfig) -> pd.DataFrame:
    path = cfg.resolved_listing_parquet()
    if not path.is_file():
        LOGGER.warning("listing parquet missing: %s — run structure listing ingest first.", path)
        return pd.DataFrame()
    df = pd.read_parquet(path)
    if "symbol" not in df.columns:
        LOGGER.warning("listing has no symbol column")
        return pd.DataFrame()
    df = df.copy()
    df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    if "exchange" in df.columns:
        df["exchange"] = df["exchange"].astype(str).str.strip().str.upper()
    else:
        df["exchange"] = "UNKNOWN"
    ex_set: set[str] = set()
    for x in cfg.exchanges:
        u = str(x).strip().upper()
        if not u:
            continue
        ex_set.add(u)
        if u == "HOSE":
            ex_set.add("HSX")
    df = df[df["exchange"].isin(ex_set)]
    return df.drop_duplicates(subset=["symbol", "exchange"])


def _download_one_pdf(
    url: str,
    dest: Path,
    *,
    session: requests.Session,
    user_agent: str,
) -> tuple[int, str | None, str | None]:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".tmp")

    def _get() -> requests.Response:
        r = session.get(
            url,
            stream=True,
            timeout=120,
            headers={"User-Agent": user_agent},
        )
        return r

    try:
        resp = call_with_retry(
            _get,
            max_attempts=4,
            base_delay_sec=1.5,
            label=f"pdf GET {dest.name}",
        )
        status = int(resp.status_code)
        if status >= 400:
            return status, None, f"http_{status}"
        h = hashlib.sha256()
        with open(tmp, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                if chunk:
                    h.update(chunk)
                    f.write(chunk)
        digest = h.hexdigest()
        peek = tmp.read_bytes()[:5]
        if not peek.startswith(b"%PDF"):
            tmp.unlink(missing_ok=True)
            return status, None, "not_pdf_magic"
        tmp.replace(dest)
        return status, digest, None
    except Exception as ex:
        tmp.unlink(missing_ok=True)
        return 0, None, f"{type(ex).__name__}: {ex}"


def ingest_bctc_pdfs(
    cfg: BctcPdfConfig | None = None,
    *,
    refresh_listing: bool = False,
    structure_cfg: StructureIngestionConfig | None = None,
    vnstock_api_key_env: str = "VNSTOCK_API_KEY",
) -> dict[str, Any]:
    """
    Discover BCTC-related PDF URLs from vnstock company news (VCI HTML), download new files,
    and append manifest rows (dedup by SHA-256).
    """
    cfg = cfg or BctcPdfConfig()
    register_vnstock_api_key_from_env(vnstock_api_key_env)

    if refresh_listing:
        scfg = structure_cfg or StructureIngestionConfig()
        ingest_listing(scfg)

    listing = _read_listing(cfg)
    if listing.empty:
        return {"manifest_partition": "", "master_manifest": str(cfg.master_manifest_path), "downloaded": 0}

    known_sha = _load_known_sha(cfg.master_manifest_path)
    session = requests.Session()

    manifest_parts: list[pd.DataFrame] = []
    n_ok = 0
    symbols = listing.to_dict("records")
    if cfg.max_symbols_per_run is not None:
        symbols = symbols[: max(0, int(cfg.max_symbols_per_run))]

    n_total = len(symbols)
    for idx, rec in enumerate(symbols, 1):
        sym = str(rec["symbol"])
        ex = str(rec["exchange"])
        LOGGER.info("ingest_bctc_pdfs: [%d/%d] %s (%s) — discovery...", idx, n_total, sym, ex)
        wait_for_rate_limit(cfg.rate_limit_rpm)
        candidates = discover_bctc_rows_vnstock(sym, cfg)
        for c in candidates:
            year = int(c.get("year") or 0)
            quarter = int(c.get("quarter") or 0)
            url = str(c["source_url"])
            sub = (
                cfg.pdf_root
                / f"exchange={ex}"
                / f"symbol={sym}"
                / f"year={year or 'unknown'}"
                / f"q={quarter or 0}"
            )
            fname = _safe_filename_from_url(url)
            dest = sub / fname
            digest: str | None = None
            status = 0
            err: str | None = None
            if dest.is_file() and dest.stat().st_size > 100:
                try:
                    digest = _hash_file(dest)
                except OSError:
                    digest = None
                if digest and digest in known_sha:
                    continue
                if digest:
                    status = 200
            if digest is None:
                status, digest, err = _download_one_pdf(
                    url,
                    dest,
                    session=session,
                    user_agent=cfg.user_agent,
                )
            row = {
                "symbol": sym,
                "exchange": ex,
                "year": year,
                "quarter": quarter,
                "title": c.get("title"),
                "source_url": url,
                "file_path": str(dest) if digest else "",
                "file_sha256": digest or "",
                "downloaded_at": datetime.now(timezone.utc).isoformat(),
                "http_status": status,
                "error": err,
                "discovery_source": c.get("discovery_source"),
                "run_date": cfg.run_date,
            }
            manifest_parts.append(pd.DataFrame([row], columns=_MANIFEST_COLS))
            if digest and not err:
                n_ok += 1
                known_sha.add(digest)

    if not manifest_parts:
        LOGGER.info("ingest_bctc_pdfs: no new manifest rows")
        return {
            "manifest_partition": "",
            "master_manifest": str(cfg.master_manifest_path),
            "downloaded": 0,
        }

    manifest_df = pd.concat(manifest_parts, ignore_index=True)
    part_path = save_partition_parquet(
        manifest_df,
        cfg.bctc_root,
        "manifest",
        cfg.run_date,
        "part-000",
    )
    _merge_master_manifest(cfg.master_manifest_path, manifest_df)
    LOGGER.info("ingest_bctc_pdfs: downloaded_ok=%s manifest=%s", n_ok, part_path)
    return {
        "manifest_partition": str(part_path),
        "master_manifest": str(cfg.master_manifest_path),
        "downloaded": n_ok,
    }


def run_bctc_pipeline(cfg: BctcPdfConfig | None = None, **kwargs: Any) -> dict[str, Any]:
    return ingest_bctc_pdfs(cfg, **kwargs)
