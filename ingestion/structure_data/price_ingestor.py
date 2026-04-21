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
    save_partition_parquet,
    transform_ohlcv,
    validate_ohlcv_frame,
    wait_for_rate_limit,
)
from .config import IngestionConfig

LOGGER = logging.getLogger(__name__)


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
    pattern = f"date=*/{filename.upper()}.parquet"
    return any(category_dir.glob(pattern))


def _resolve_price_fetch_range(symbol: str, cfg: IngestionConfig) -> tuple[str, str, str]:
    end = cfg.end_date
    if not cfg.use_incremental_window:
        return cfg.start_date, end, f"full_{cfg.years_back}y"

    if _is_full_bootstrap_once_enabled(cfg) and not _has_full_bootstrap_marker(cfg, "price"):
        return cfg.start_date, end, f"bootstrap_full_once_{cfg.years_back}y"

    window_days = max(int(cfg.incremental_window_days), 1)
    has_existing = _has_existing_partition_file(cfg, "price", symbol)
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
        return incremental_min_rows
    return full_min_rows


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


def ingest_prices(cfg: IngestionConfig | None = None) -> dict[str, str]:
    cfg = cfg or IngestionConfig()
    outputs: dict[str, str] = {}
    n = min(len(cfg.tickers), cfg.max_tickers_per_run)
    for idx, symbol in enumerate(cfg.tickers[: cfg.max_tickers_per_run]):
        start, end, range_mode = _resolve_price_fetch_range(symbol, cfg)
        min_rows_required = _resolve_price_min_rows(range_mode, cfg)
        LOGGER.info(
            "[%s/%s] Fetching %s (%s: %s -> %s, qc_min_rows=%s)...",
            idx + 1,
            n,
            symbol,
            range_mode,
            start,
            end,
            min_rows_required,
        )
        cleaned, src = _fetch_history_with_fallback(
            symbol,
            start,
            end,
            cfg,
            min_rows_required=min_rows_required,
        )
        if cleaned.empty:
            LOGGER.warning("Skip %s: không có dữ liệu hợp lệ từ mọi nguồn", symbol)
            continue
        final_df = build_price_like_schema(
            cleaned, symbol, cfg.run_date, source=src, instrument_type="stock"
        )
        log_ohlcv_quality(symbol, final_df, src)
        out_file = save_partition_parquet(
            final_df, cfg.data_lake_root, "price", cfg.run_date, symbol
        )
        outputs[symbol] = str(out_file)

    if _is_full_bootstrap_once_enabled(cfg) and not _has_full_bootstrap_marker(cfg, "price"):
        _write_full_bootstrap_marker(
            cfg,
            "price",
            requested=n,
            succeeded=len(outputs),
        )

    return outputs
