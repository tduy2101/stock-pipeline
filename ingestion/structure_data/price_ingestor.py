from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from vnstock import Quote

from .common import (
    build_price_like_schema,
    call_with_retry,
    log_ohlcv_quality,
    next_date_text,
    resolve_trading_date_watermark,
    save_monthly_ticker_parquets,
    transform_ohlcv,
    validate_ohlcv_frame,
    wait_for_rate_limit,
)
from .config import IngestionConfig

LOGGER = logging.getLogger(__name__)
_WATERMARK_UNSET = object()


def _is_full_bootstrap_once_enabled(cfg: IngestionConfig) -> bool:
    return bool(
        getattr(cfg, "full_bootstrap_once_then_incremental", False)
        and getattr(cfg, "use_incremental_window", True)
    )


def _full_bootstrap_marker_path(cfg: IngestionConfig, category: str) -> Path:
    if hasattr(cfg, "full_bootstrap_marker_path"):
        return cfg.full_bootstrap_marker_path(category)
    marker_file = str(getattr(cfg, "full_bootstrap_marker_file", "_full_bootstrap_done.json"))
    return cfg.data_lake_root / category / marker_file


def _has_full_bootstrap_marker(cfg: IngestionConfig, category: str) -> bool:
    return _full_bootstrap_marker_path(cfg, category).exists()


def _write_full_bootstrap_marker(
    cfg: IngestionConfig,
    category: str,
    *,
    requested: int,
    succeeded: int,
) -> None:
    path = _full_bootstrap_marker_path(cfg, category)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "category": category,
        "mode": "full_bootstrap_once_then_incremental",
        "run_date": cfg.run_date,
        "requested": int(requested),
        "succeeded": int(succeeded),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    LOGGER.info("%s: đã tạo bootstrap marker -> %s", category, path)


def _has_existing_partition_file(
    cfg: IngestionConfig, category: str, filename: str
) -> bool:
    category_dir = cfg.data_lake_root / category
    if not category_dir.exists():
        return False
    pattern = f"year=*/month=*/{filename.upper()}.parquet"
    return any(category_dir.glob(pattern))


def _resolve_price_fetch_range(
    symbol: str,
    cfg: IngestionConfig,
    watermark: str | None | object = _WATERMARK_UNSET,
) -> tuple[str, str, str]:
    end = cfg.end_date
    if not cfg.use_incremental_window:
        return cfg.start_date, end, f"full_{cfg.years_back}y"

    if _is_full_bootstrap_once_enabled(cfg) and not _has_full_bootstrap_marker(cfg, "price"):
        return cfg.start_date, end, f"bootstrap_full_once_{cfg.years_back}y"

    window_days = max(int(cfg.incremental_window_days), 1)
    has_existing = _has_existing_partition_file(cfg, "price", symbol)
    if watermark is _WATERMARK_UNSET:
        watermark = resolve_trading_date_watermark(
            raw_root=cfg.data_lake_root,
            dataset="price",
            silver_dataset="price",
            gold_tables=("gold.fact_price", "gold.mart_stock_daily"),
        )
    if watermark and has_existing:
        return watermark, end, "incremental_watermark"

    if has_existing:
        start = (date.today() - timedelta(days=window_days)).isoformat()
        return start, end, f"incremental_{window_days}d"

    if cfg.bootstrap_full_history_if_missing:
        return cfg.start_date, end, f"bootstrap_full_{cfg.years_back}y"

    start = (date.today() - timedelta(days=window_days)).isoformat()
    return start, end, f"bootstrap_incremental_{window_days}d"


def _resolve_price_min_rows(range_mode: str, cfg: IngestionConfig) -> int:
    full_min_rows = max(1, int(getattr(cfg, "min_ohlcv_rows_stock", 50)))
    incremental_min_rows = max(
        1,
        int(getattr(cfg, "min_ohlcv_rows_stock_incremental", 5)),
    )
    if range_mode.startswith("incremental_") or range_mode.startswith(
        "bootstrap_incremental_"
    ):
        window_days = max(1, int(getattr(cfg, "incremental_window_days", 1)))
        return min(incremental_min_rows, window_days)
    return full_min_rows


def _iter_fetch_windows(start: str, end: str) -> list[tuple[str, str]]:
    start_dt = date.fromisoformat(start)
    end_dt = date.fromisoformat(end)
    if (end_dt - start_dt).days <= 366:
        return [(start, end)]

    windows: list[tuple[str, str]] = []
    cursor = start_dt
    while cursor <= end_dt:
        window_end = min(date(cursor.year, 12, 31), end_dt)
        windows.append((cursor.isoformat(), window_end.isoformat()))
        cursor = window_end + timedelta(days=1)
    return windows


def _fetch_history_with_fallback(
    symbol: str,
    start: str,
    end: str,
    cfg: IngestionConfig,
    *,
    min_rows_required: int,
) -> tuple[pd.DataFrame, str]:
    for src in cfg.resolved_data_sources():

        def _pull() -> pd.DataFrame:
            wait_for_rate_limit(cfg.rate_limit_rpm)
            quote = Quote(source=src, symbol=symbol)
            return quote.history(start=start, end=end, interval="1D")

        try:
            raw = call_with_retry(
                _pull,
                max_attempts=cfg.api_retry_max_attempts,
                base_delay_sec=cfg.api_retry_base_delay_sec,
                label=f"{symbol}@{src}",
            )
        except Exception as ex:
            LOGGER.warning("Fetch %s from %s failed: %s", symbol, src, ex)
            continue
        if raw is None or raw.empty:
            LOGGER.warning("Fetch %s from %s: empty response", symbol, src)
            continue
        cleaned = transform_ohlcv(raw)
        ok, reason = validate_ohlcv_frame(
            cleaned,
            min_rows=min_rows_required,
        )
        if ok:
            return cleaned, src
        LOGGER.warning(
            "QC không đạt %s từ %s (%s) — thử nguồn khác nếu có",
            symbol,
            src,
            reason,
        )
    return pd.DataFrame(), ""


def ingest_prices(cfg: IngestionConfig | None = None) -> dict[str, list[str]]:
    cfg = cfg or IngestionConfig()
    outputs: dict[str, list[str]] = {}
    n = min(len(cfg.tickers), cfg.max_tickers_per_run)
    watermark = resolve_trading_date_watermark(
        raw_root=cfg.data_lake_root,
        dataset="price",
        silver_dataset="price",
        gold_tables=("gold.fact_price", "gold.mart_stock_daily"),
    )
    for idx, symbol in enumerate(cfg.tickers[: cfg.max_tickers_per_run]):
        start, end, range_mode = _resolve_price_fetch_range(symbol, cfg, watermark)
        if start > end:
            LOGGER.info(
                "[%s/%s] Skip %s: watermark is already past end_date (%s > %s)",
                idx + 1,
                n,
                symbol,
                start,
                end,
            )
            continue
        min_rows_required = _resolve_price_min_rows(range_mode, cfg)
        symbol_files: list[str] = []
        for window_start, window_end in _iter_fetch_windows(start, end):
            LOGGER.info(
                "[%s/%s] Fetching %s (%s: %s -> %s, qc_min_rows=%s)...",
                idx + 1,
                n,
                symbol,
                range_mode,
                window_start,
                window_end,
                min_rows_required,
            )
            cleaned, src = _fetch_history_with_fallback(
                symbol,
                window_start,
                window_end,
                cfg,
                min_rows_required=min_rows_required,
            )
            if cleaned.empty:
                LOGGER.warning(
                    "Skip %s window %s -> %s: không có dữ liệu hợp lệ từ mọi nguồn",
                    symbol,
                    window_start,
                    window_end,
                )
                continue
            final_df = build_price_like_schema(
                cleaned, symbol, cfg.run_date, source=src, instrument_type="stock"
            )
            log_ohlcv_quality(symbol, final_df, src)
            out_files = save_monthly_ticker_parquets(
                final_df, cfg.data_lake_root, "price", cfg.run_date, symbol
            )
            symbol_files.extend(str(path) for path in out_files)
        if not symbol_files:
            LOGGER.warning("Skip %s: không có dữ liệu hợp lệ từ mọi nguồn", symbol)
            continue
        outputs[symbol] = symbol_files
    if _is_full_bootstrap_once_enabled(cfg) and not _has_full_bootstrap_marker(cfg, "price"):
        _write_full_bootstrap_marker(
            cfg,
            "price",
            requested=n,
            succeeded=len(outputs),
        )

    return outputs
