from __future__ import annotations

from typing import Any

from .bctc_annual_pdf_ingestor import ingest_bctc_annual_pdfs
from .config import SemiStructuredIngestionConfig


def run_bctc_annual_pipeline(
    cfg: SemiStructuredIngestionConfig | None = None,
    *,
    include_download: bool = True,
) -> dict[str, Any]:
    """Run BCTC PDF ingestion (download-only)."""
    cfg = cfg or SemiStructuredIngestionConfig()
    out: dict[str, Any] = {"run_date": cfg.run_date}

    if include_download:
        out["download"] = ingest_bctc_annual_pdfs(cfg)

    return out
