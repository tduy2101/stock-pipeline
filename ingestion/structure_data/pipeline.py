from __future__ import annotations

import logging
import time
from typing import Any

from .config import IngestionConfig
from .index_ingestor import ingest_indices
from .price_ingestor import ingest_prices
from .stock_info_ingestor import (
    ingest_company_overview,
    ingest_financial_ratio,
    ingest_listing,
    ingest_price_board,
)

LOGGER = logging.getLogger(__name__)


def run_structure_ingestion_pipeline(
    cfg: IngestionConfig | None = None,
    *,
    include_prices: bool = True,
    include_indices: bool = True,
    include_listing: bool = True,
    include_company: bool = True,
    include_financial_ratio: bool = False,
    include_price_board: bool = True,
) -> dict[str, Any]:
    """Chạy tuần tự các bước ingest, nghỉ giữa các nhóm để giảm lỗi mạng / rate limit."""
    cfg = cfg or IngestionConfig()
    out: dict[str, Any] = {}
    delay = max(0, int(cfg.delay_between_categories_sec))

    def _pause() -> None:
        if delay > 0:
            LOGGER.info("Chờ %ss trước bước tiếp theo...", delay)
            time.sleep(delay)

    if include_prices:
        out["price"] = ingest_prices(cfg)
        _pause()
    if include_indices:
        out["index"] = ingest_indices(cfg)
        _pause()
    if include_listing:
        out["listing"] = ingest_listing(cfg)
        _pause()
    if include_company:
        out["company"] = ingest_company_overview(cfg)
        _pause()
    if include_financial_ratio:
        out["financial_ratio"] = ingest_financial_ratio(cfg)
        _pause()
    if include_price_board:
        out["price_board"] = ingest_price_board(cfg)
    return out


def run_financial_ratio_ingestion_pipeline(
    cfg: IngestionConfig | None = None,
) -> dict[str, Any]:
    """Chạy riêng financial_ratio để gắn vào schedule độc lập (weekly/monthly)."""
    cfg = cfg or IngestionConfig()
    return {"financial_ratio": ingest_financial_ratio(cfg)}


def run_structure_full_ingestion_pipeline(
    cfg: IngestionConfig | None = None,
) -> dict[str, Any]:
    """Giữ hành vi đầy đủ cũ: chạy cả financial_ratio trong cùng pipeline."""
    return run_structure_ingestion_pipeline(cfg, include_financial_ratio=True)
