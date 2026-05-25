from __future__ import annotations

import logging
import time
from datetime import date
from pathlib import Path
from typing import Any

from .common import (
    month_partition_value_from_path,
    partition_value_from_path,
    write_run_metadata,
)
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


def _flatten_output_paths(outputs: object) -> list[Path]:
    if isinstance(outputs, dict):
        paths: list[Path] = []
        for value in outputs.values():
            paths.extend(_flatten_output_paths(value))
        return paths
    if isinstance(outputs, (list, tuple, set)):
        paths = []
        for value in outputs:
            paths.extend(_flatten_output_paths(value))
        return paths
    if outputs:
        return [Path(str(outputs))]
    return []


def _infer_ohlcv_run_type(cfg: IngestionConfig, paths: list[Path]) -> str:
    if not getattr(cfg, "use_incremental_window", True):
        return "backfill"
    trading_dates = [
        value
        for path in paths
        for value in [partition_value_from_path(path)]
        if value
    ]
    if not trading_dates:
        trading_dates = [
            f"{value}-01"
            for path in paths
            for value in [month_partition_value_from_path(path)]
            if value
        ]
    if not trading_dates:
        return "incremental"
    first = date.fromisoformat(min(trading_dates))
    last = date.fromisoformat(max(trading_dates))
    max_incremental_span = max(1, int(getattr(cfg, "incremental_window_days", 1))) + 7
    return "backfill" if (last - first).days > max_incremental_span else "incremental"


def _write_ohlcv_run_metadata(
    cfg: IngestionConfig,
    category: str,
    outputs: dict[str, object],
    *,
    requested_count: int,
) -> None:
    paths = _flatten_output_paths(outputs)
    write_run_metadata(
        cfg.data_lake_root,
        category,
        run_id=cfg.run_date,
        run_type=_infer_ohlcv_run_type(cfg, paths),
        tickers=sorted(outputs.keys()),
        paths=paths,
        requested_count=requested_count,
    )


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
        _write_ohlcv_run_metadata(
            cfg,
            "price",
            out["price"],
            requested_count=min(len(cfg.tickers), cfg.max_tickers_per_run),
        )
        _pause()
    if include_indices:
        out["index"] = ingest_indices(cfg)
        _write_ohlcv_run_metadata(
            cfg,
            "index",
            out["index"],
            requested_count=len(cfg.index_tickers),
        )
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
