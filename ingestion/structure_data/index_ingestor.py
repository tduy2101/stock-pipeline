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


def _resolve_index_fetch_range(index_code: str, cfg: IngestionConfig) -> tuple[str, str, str]:
    end = cfg.end_date
    if not cfg.use_incremental_window:
        return cfg.start_date, end, f"full_{cfg.years_back}y"

    if _is_full_bootstrap_once_enabled(cfg) and not _has_full_bootstrap_marker(cfg, "index"):
        return cfg.start_date, end, f"bootstrap_full_once_{cfg.years_back}y"

    window_days = max(int(cfg.incremental_window_days), 1)
    has_existing = _has_existing_partition_file(cfg, "index", index_code)
    if has_existing:
        start = (date.today() - timedelta(days=window_days)).isoformat()
        return start, end, f"incremental_{window_days}d"

    if cfg.bootstrap_full_history_if_missing:
        return cfg.start_date, end, f"bootstrap_full_{cfg.years_back}y"

    start = (date.today() - timedelta(days=window_days)).isoformat()
    return start, end, f"bootstrap_incremental_{window_days}d"


def _resolve_index_min_rows(range_mode: str, cfg: IngestionConfig) -> int:
    full_min_rows = max(1, int(getattr(cfg, "min_ohlcv_rows_index", 100)))
    incremental_min_rows = max(
        1,
        int(getattr(cfg, "min_ohlcv_rows_index_incremental", 5)),
    )
    if range_mode.startswith("incremental_") or range_mode.startswith(
        "bootstrap_incremental_"
    ):
        return incremental_min_rows
    return full_min_rows


def ingest_indices(cfg: IngestionConfig | None = None) -> dict[str, str]:
    cfg = cfg or IngestionConfig()
    outputs: dict[str, str] = {}
    sources = cfg.resolved_data_sources()
    primary = sources[0] if sources else cfg.primary_source
    fallback = sources[1] if len(sources) > 1 else None

    for j, index_code in enumerate(cfg.index_tickers):
        start, end, range_mode = _resolve_index_fetch_range(index_code, cfg)
        min_rows_required = _resolve_index_min_rows(range_mode, cfg)
        LOGGER.info(
            "[%s/%s] Fetching index %s (%s: %s -> %s, qc_min_rows=%s)...",
            j + 1,
            len(cfg.index_tickers),
            index_code,
            range_mode,
            start,
            end,
            min_rows_required,
        )
        selected = pd.DataFrame()
        src_used = ""
        primary_fail_reason: str | None = None
        last_reason = None

        for src in sources:

            def _pull() -> pd.DataFrame:
                wait_for_rate_limit(cfg.rate_limit_rpm)
                quote = Quote(source=src, symbol=index_code)
                return quote.history(start=start, end=end, interval="1D")

            try:
                raw = call_with_retry(
                    _pull,
                    max_attempts=cfg.api_retry_max_attempts,
                    base_delay_sec=cfg.api_retry_base_delay_sec,
                    label=f"{index_code}@{src}",
                )
            except Exception as ex:
                last_reason = str(ex)[:120]
                if src == primary:
                    primary_fail_reason = last_reason
                    LOGGER.warning(
                        "fallback %s -> %s cho index %s (lỗi từ %s)",
                        primary,
                        fallback or "?",
                        index_code,
                        primary,
                    )
                LOGGER.warning("Fetch index %s from %s failed: %s", index_code, src, ex)
                continue

            if raw is None or raw.empty:
                last_reason = "empty"
                if src == primary:
                    primary_fail_reason = "empty"
                    LOGGER.warning(
                        "fallback %s -> %s cho index %s (%s trả rỗng)",
                        primary,
                        fallback or "?",
                        index_code,
                        primary,
                    )
                continue

            cleaned = transform_ohlcv(raw)
            ok, reason = validate_ohlcv_frame(
                cleaned,
                min_rows=min_rows_required,
            )
            if ok:
                selected = cleaned
                src_used = src
                if src == fallback and primary_fail_reason:
                    LOGGER.info(
                        "index %s dùng %s sau khi %s không đạt (%s)",
                        index_code,
                        src,
                        primary,
                        primary_fail_reason,
                    )
                break

            last_reason = reason
            if src == primary:
                primary_fail_reason = reason
                LOGGER.warning(
                    "fallback %s -> %s cho index %s (QC %s fail: %s)",
                    primary,
                    fallback or "?",
                    index_code,
                    primary,
                    reason,
                )
            else:
                LOGGER.warning("index %s: QC %s fail (%s)", index_code, src, reason)

        if selected.empty:
            LOGGER.warning(
                "Bỏ qua index %s — không có nguồn nào đạt (lý do cuối: %s)",
                index_code,
                last_reason,
            )
            continue

        final_df = build_price_like_schema(
            selected, index_code, cfg.run_date, source=src_used, instrument_type="index"
        )
        log_ohlcv_quality(index_code, final_df, src_used)
        out_file = save_partition_parquet(
            final_df, cfg.data_lake_root, "index", cfg.run_date, index_code
        )
        outputs[index_code] = str(out_file)

    if _is_full_bootstrap_once_enabled(cfg) and not _has_full_bootstrap_marker(cfg, "index"):
        _write_full_bootstrap_marker(
            cfg,
            "index",
            requested=len(cfg.index_tickers),
            succeeded=len(outputs),
        )

    return outputs
