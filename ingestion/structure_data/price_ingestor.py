from __future__ import annotations

import logging

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


def _fetch_history_with_fallback(
    symbol: str, start: str, end: str, cfg: IngestionConfig
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
            cleaned, min_rows=cfg.min_ohlcv_rows_stock
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
        LOGGER.info("[%s/%s] Fetching %s...", idx + 1, n, symbol)
        cleaned, src = _fetch_history_with_fallback(
            symbol, cfg.start_date, cfg.end_date, cfg
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
    return outputs
