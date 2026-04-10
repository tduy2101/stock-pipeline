from __future__ import annotations

import logging

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


def ingest_indices(cfg: IngestionConfig | None = None) -> dict[str, str]:
    cfg = cfg or IngestionConfig()
    outputs: dict[str, str] = {}
    sources = cfg.resolved_data_sources()
    primary = sources[0] if sources else cfg.primary_source
    fallback = sources[1] if len(sources) > 1 else None

    for j, index_code in enumerate(cfg.index_tickers):
        LOGGER.info("[%s/%s] Fetching index %s...", j + 1, len(cfg.index_tickers), index_code)
        selected = pd.DataFrame()
        src_used = ""
        primary_fail_reason: str | None = None
        last_reason = None

        for src in sources:

            def _pull() -> pd.DataFrame:
                wait_for_rate_limit(cfg.rate_limit_rpm)
                quote = Quote(source=src, symbol=index_code)
                return quote.history(start=cfg.start_date, end=cfg.end_date, interval="1D")

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
                cleaned, min_rows=cfg.min_ohlcv_rows_index
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
    return outputs
