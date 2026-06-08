from __future__ import annotations

import logging
import time
from dataclasses import replace
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from .common import (
    load_tickers_from_listing_bronze,
    load_tickers_from_listing_bronze_in_file_order,
    month_partition_value_from_path,
    partition_value_from_path,
    write_run_metadata,
)
from .config import IngestionConfig
from .index_ingestor import ingest_indices
from .price_ingestor import ingest_prices
from .stock_info_ingestor import (
    fetch_price_board_frame,
    ingest_company_overview,
    ingest_financial_ratio,
    ingest_listing,
    ingest_price_board,
    save_price_board_frame,
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


def _active_tickers(cfg: IngestionConfig) -> list[str]:
    return [str(t).strip().upper() for t in cfg.tickers[: cfg.max_tickers_per_run] if str(t).strip()]


def _chunk_tickers(tickers: list[str], batch_size: int) -> list[list[str]]:
    size = max(1, int(batch_size))
    return [tickers[i : i + size] for i in range(0, len(tickers), size)]


def _ingest_price_batched(cfg: IngestionConfig) -> dict[str, Any]:
    tickers = _active_tickers(cfg)
    batches = _chunk_tickers(tickers, cfg.price_batch_size)
    merged: dict[str, list[str]] = {}
    failed_batches: list[dict[str, Any]] = []
    batches_ok = 0
    total = len(batches)

    for batch_idx, batch in enumerate(batches):
        first = batch[0]
        last = batch[-1]
        LOGGER.info(
            "Price batch %s/%s: %s tickers [%s...%s]",
            batch_idx + 1,
            total,
            len(batch),
            first,
            last,
        )
        cfg_batch = replace(cfg, tickers=batch, max_tickers_per_run=len(batch))
        try:
            batch_out = ingest_prices(cfg_batch)
            merged.update(batch_out)
            batches_ok += 1
        except Exception as ex:
            LOGGER.warning(
                "Price batch %s/%s failed (%s tickers [%s...%s]): %s",
                batch_idx + 1,
                total,
                len(batch),
                first,
                last,
                ex,
            )
            failed_batches.append(
                {
                    "batch_idx": batch_idx,
                    "tickers": batch,
                    "error": str(ex),
                }
            )
        if batch_idx < total - 1 and cfg.delay_between_batches_sec > 0:
            time.sleep(cfg.delay_between_batches_sec)

    LOGGER.info(
        "Price ingestion done: %s batches ok, %s failed",
        batches_ok,
        len(failed_batches),
    )
    return {
        "outputs": merged,
        "batches_total": total,
        "batches_ok": batches_ok,
        "batches_failed": len(failed_batches),
        "failed_batches": failed_batches,
    }


def _price_board_tickers(cfg: IngestionConfig) -> list[str]:
    """
    Symbols for price board.

    ``price_board_use_listing_universe=True``: first N from ``cfg.tickers`` (listing).
    Otherwise: ``cfg.price_board_tickers`` watchlist (DAG default 50 mã cố định).
    """
    cap = max(1, int(cfg.price_board_max_tickers))
    if bool(getattr(cfg, "price_board_use_listing_universe", False)):
        return _active_tickers(cfg)[:cap]
    seen: set[str] = set()
    out: list[str] = []
    for raw in cfg.price_board_tickers:
        sym = str(raw).strip().upper()
        if not sym or sym in seen:
            continue
        seen.add(sym)
        out.append(sym)
        if len(out) >= cap:
            break
    return out


def _empty_price_board_result() -> dict[str, Any]:
    return {
        "outputs": [],
        "batches_total": 0,
        "batches_ok": 0,
        "batches_failed": 0,
        "failed_batches": [],
        "snapshot_at": None,
        "tickers_requested": 0,
    }


def _ingest_price_board_snapshot(cfg: IngestionConfig) -> dict[str, Any]:
    """
    One Bronze partition ``snapshot_at=<run_time>`` per pipeline run.

    Mặc định: listing universe + batched fetch, gộp một ``snapshot_at``.
    Watchlist 50 mã: ``price_board_use_listing_universe=False``,
    ``price_board_batched=False``, ``price_board_max_tickers=50``.
    """
    tickers = _price_board_tickers(cfg)
    if not tickers:
        LOGGER.warning("Price board: no tickers after cap; skip.")
        return _empty_price_board_result()

    snapshot_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
    use_batches = bool(cfg.price_board_batched) and len(tickers) > int(cfg.price_board_batch_size)

    if not use_batches:
        cfg_run = replace(cfg, tickers=tickers, max_tickers_per_run=len(tickers))
        try:
            path = ingest_price_board(cfg_run, snapshot_at=snapshot_at)
        except Exception as ex:
            LOGGER.warning("Price board single snapshot failed (%s tickers): %s", len(tickers), ex)
            return {
                **_empty_price_board_result(),
                "batches_total": 1,
                "batches_failed": 1,
                "failed_batches": [{"batch_idx": 0, "tickers": tickers, "error": str(ex)}],
                "snapshot_at": snapshot_at,
                "tickers_requested": len(tickers),
            }
        outputs = [path] if path else []
        LOGGER.info(
            "Price board snapshot done: tickers=%s snapshot_at=%s rows_saved=%s",
            len(tickers),
            snapshot_at,
            "yes" if path else "no",
        )
        return {
            "outputs": outputs,
            "batches_total": 1,
            "batches_ok": 1 if path else 0,
            "batches_failed": 0 if path else 1,
            "failed_batches": [] if path else [{"tickers": tickers, "error": "empty_snapshot"}],
            "snapshot_at": snapshot_at,
            "tickers_requested": len(tickers),
        }

    batches = _chunk_tickers(tickers, cfg.price_board_batch_size)
    frames: list[pd.DataFrame] = []
    src_used = ""
    failed_batches: list[dict[str, Any]] = []
    batches_ok = 0
    total = len(batches)

    for batch_idx, batch in enumerate(batches):
        LOGGER.info(
            "Price board fetch batch %s/%s: %s tickers [%s...%s] (merge -> %s)",
            batch_idx + 1,
            total,
            len(batch),
            batch[0],
            batch[-1],
            snapshot_at,
        )
        try:
            frame, src = fetch_price_board_frame(cfg, batch)
            if frame.empty:
                raise ValueError("empty price_board frame")
            frames.append(frame)
            if src and not src_used:
                src_used = src
            batches_ok += 1
        except Exception as ex:
            LOGGER.warning(
                "Price board fetch batch %s/%s failed: %s",
                batch_idx + 1,
                total,
                ex,
            )
            failed_batches.append(
                {"batch_idx": batch_idx, "tickers": batch, "error": str(ex)}
            )
        if batch_idx < total - 1 and cfg.delay_between_batches_sec > 0:
            time.sleep(cfg.delay_between_batches_sec)

    if not frames:
        return {
            **_empty_price_board_result(),
            "batches_total": total,
            "batches_failed": len(failed_batches),
            "failed_batches": failed_batches,
            "snapshot_at": snapshot_at,
            "tickers_requested": len(tickers),
        }

    combined = pd.concat(frames, ignore_index=True, sort=False)
    path = save_price_board_frame(
        cfg, combined, snapshot_at=snapshot_at, data_source=src_used or cfg.primary_source
    )
    outputs = [path] if path else []
    LOGGER.info(
        "Price board merged snapshot: tickers=%s batches_ok=%s/%s snapshot_at=%s",
        len(tickers),
        batches_ok,
        total,
        snapshot_at,
    )
    return {
        "outputs": outputs,
        "batches_total": total,
        "batches_ok": batches_ok,
        "batches_failed": len(failed_batches),
        "failed_batches": failed_batches,
        "snapshot_at": snapshot_at,
        "tickers_requested": len(tickers),
    }


# Backward-compatible alias (tests / external imports)
_ingest_price_board_batched = _ingest_price_board_snapshot


def _ingest_financial_ratio_filtered(cfg: IngestionConfig) -> dict[str, Any]:
    """
    Run financial-ratio ingest for tickers matching ``financial_ratio_exchange_filter``.

    When the exchange filter is empty, uses all active ``cfg.tickers``.
    """
    exchange_filter = cfg.financial_ratio_exchange_filter
    if exchange_filter:
        active_set = set(_active_tickers(cfg))
        exchange_tickers = load_tickers_from_listing_bronze_in_file_order(
            cfg,
            exchange_filter=exchange_filter,
            security_type_filter=[],
        )
        tickers = [t for t in exchange_tickers if t in active_set]
    else:
        tickers = _active_tickers(cfg)

    LOGGER.info(
        "Financial ratio: %s tickers after exchange filter %s",
        len(tickers),
        exchange_filter or "[]",
    )
    if not tickers:
        return {"financial_ratio": {}}

    cfg_filtered = replace(cfg, tickers=tickers, max_tickers_per_run=len(tickers))
    return {"financial_ratio": ingest_financial_ratio(cfg_filtered)}


def _apply_listing_universe(cfg: IngestionConfig) -> None:
    cfg.tickers = load_tickers_from_listing_bronze(cfg)
    cap = getattr(cfg, "listing_max_tickers", None)
    if cap is not None and int(cap) > 0:
        cfg.tickers = cfg.tickers[: int(cap)]
    cfg.max_tickers_per_run = len(cfg.tickers)
    exchange_label = cfg.listing_exchange_filter or "[]"
    security_label = cfg.listing_security_type_filter or "[]"
    LOGGER.info(
        "Loaded %s tickers from listing (exchange=%s security_type=%s)",
        len(cfg.tickers),
        exchange_label,
        security_label,
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

    use_listing_universe = bool(getattr(cfg, "use_listing_as_universe", False))

    if use_listing_universe:
        if include_listing:
            out["listing"] = ingest_listing(cfg)
            _pause()
        _apply_listing_universe(cfg)
        if include_company:
            out["company"] = ingest_company_overview(cfg)
            _pause()
        if include_prices:
            price_result = _ingest_price_batched(cfg)
            out["price"] = price_result["outputs"]
            out["price_batches"] = {
                k: price_result[k]
                for k in (
                    "batches_total",
                    "batches_ok",
                    "batches_failed",
                    "failed_batches",
                )
            }
            _write_ohlcv_run_metadata(
                cfg,
                "price",
                out["price"],
                requested_count=len(_active_tickers(cfg)),
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
        if include_price_board:
            board_result = _ingest_price_board_snapshot(cfg)
            out["price_board"] = board_result["outputs"]
            out["price_board_batches"] = {
                k: board_result[k]
                for k in (
                    "batches_total",
                    "batches_ok",
                    "batches_failed",
                    "failed_batches",
                )
            }
        if include_financial_ratio:
            out.update(_ingest_financial_ratio_filtered(cfg))
        return out

    if include_prices:
        price_result = _ingest_price_batched(cfg)
        out["price"] = price_result["outputs"]
        out["price_batches"] = {
            k: price_result[k]
            for k in (
                "batches_total",
                "batches_ok",
                "batches_failed",
                "failed_batches",
            )
        }
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
        out.update(_ingest_financial_ratio_filtered(cfg))
        _pause()
    if include_price_board:
        board_result = _ingest_price_board_snapshot(cfg)
        out["price_board"] = board_result["outputs"]
        out["price_board_batches"] = {
            k: board_result[k]
            for k in (
                "batches_total",
                "batches_ok",
                "batches_failed",
                "failed_batches",
            )
        }
    return out


def run_financial_ratio_ingestion_pipeline(
    cfg: IngestionConfig | None = None,
) -> dict[str, Any]:
    """Chạy riêng financial_ratio để gắn vào schedule độc lập (weekly/monthly)."""
    cfg = cfg or IngestionConfig()
    return _ingest_financial_ratio_filtered(cfg)


def run_structure_full_ingestion_pipeline(
    cfg: IngestionConfig | None = None,
) -> dict[str, Any]:
    """Giữ hành vi đầy đủ cũ: chạy cả financial_ratio trong cùng pipeline."""
    return run_structure_ingestion_pipeline(cfg, include_financial_ratio=True)
