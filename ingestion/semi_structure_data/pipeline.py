from __future__ import annotations

import logging
import time
from typing import Any

from .bctc_annual_pdf_ingestor import ingest_bctc_annual_pdfs
from .bctc_annual_pdf_parser import parse_bctc_annual_pdfs
from .config import SemiStructuredIngestionConfig

LOGGER = logging.getLogger(__name__)


def run_bctc_annual_pipeline(
    cfg: SemiStructuredIngestionConfig | None = None,
    *,
    include_download: bool = True,
    include_parse: bool = True,
) -> dict[str, Any]:
    cfg = cfg or SemiStructuredIngestionConfig()
    out: dict[str, Any] = {"run_date": cfg.run_date}

    if include_download:
        out["download"] = ingest_bctc_annual_pdfs(cfg)

    if include_download and include_parse and cfg.delay_between_categories_sec > 0:
        delay = int(cfg.delay_between_categories_sec)
        LOGGER.info("Cho %ss truoc khi sang stage parse...", delay)
        time.sleep(delay)

    if include_parse:
        out["parse"] = parse_bctc_annual_pdfs(cfg)

    return out

