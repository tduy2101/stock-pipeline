from __future__ import annotations

import logging
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, TypeVar

import pandas as pd

LOGGER = logging.getLogger(__name__)
_last_request_time = 0.0

T = TypeVar("T")


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def wait_for_rate_limit(rate_limit_rpm: int) -> None:
    global _last_request_time
    now = time.time()
    elapsed = now - _last_request_time
    min_interval = 60.0 / max(rate_limit_rpm, 1)
    if elapsed < min_interval:
        time.sleep(min_interval - elapsed)
    _last_request_time = time.time()


def is_retryable_error(exc: BaseException) -> bool:
    if isinstance(
        exc,
        (
            ConnectionError,
            TimeoutError,
            OSError,
        ),
    ):
        return True
    name = type(exc).__name__
    if name in ("RetryError", "ReadTimeout", "ConnectTimeout", "ChunkedEncodingError"):
        return True
    msg = str(exc).lower()
    if "connection" in msg or "timeout" in msg or "temporar" in msg:
        return True
    return False


def call_with_retry(
    fn: Callable[[], T],
    *,
    max_attempts: int,
    base_delay_sec: float,
    label: str = "",
) -> T:
    last: BaseException | None = None
    for attempt in range(max(1, max_attempts)):
        try:
            return fn()
        except Exception as ex:
            last = ex
            if attempt >= max_attempts - 1 or not is_retryable_error(ex):
                raise
            delay = base_delay_sec * (2**attempt) + random.uniform(0, 0.35)
            prefix = f"{label} " if label else ""
            LOGGER.warning(
                "%sattempt %s/%s failed (%s), retry in %.2fs",
                prefix,
                attempt + 1,
                max_attempts,
                type(ex).__name__,
                delay,
            )
            time.sleep(delay)
    assert last is not None
    raise last


def project_root() -> Path:
    """Thư mục gốc repo (stock-pipeline/), tính từ vị trí file này."""
    return Path(__file__).resolve().parents[2]


def load_dotenv_from_project_root() -> bool:
    try:
        from dotenv import load_dotenv
    except ImportError:
        LOGGER.debug("python-dotenv chưa cài — bỏ qua file .env")
        return False
    env_path = project_root() / ".env"
    if not env_path.is_file():
        LOGGER.debug("Không thấy %s — bỏ qua", env_path)
        return False
    load_dotenv(env_path)
    LOGGER.info("Đã nạp biến môi trường từ %s", env_path)
    return True


def register_vnstock_api_key_from_env(env_var: str = "VNSTOCK_API_KEY") -> bool:
    import os

    load_dotenv_from_project_root()
    key = os.environ.get(env_var, "").strip()
    if not key:
        LOGGER.info("Biến môi trường %s trống — bỏ qua register_user.", env_var)
        return False
    try:
        from vnstock import register_user

        register_user(key)
        LOGGER.info("Đã gọi register_user từ %s.", env_var)
        return True
    except Exception as ex:
        LOGGER.warning("register_user thất bại: %s", ex)
        return False


def transform_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [c.strip().lower().replace(" ", "_") for c in out.columns]
    if "tradingdate" not in out.columns and "time" in out.columns:
        out = out.rename(columns={"time": "tradingdate"})
    out = out.rename(columns={"tradingdate": "tradingDate"})
    if "tradingDate" in out.columns:
        out["tradingDate"] = pd.to_datetime(
            out["tradingDate"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")
    if "tradingdate" in out.columns:
        out["tradingdate"] = pd.to_datetime(
            out["tradingdate"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")
    return out


def validate_ohlcv_frame(
    df: pd.DataFrame, *, min_rows: int = 200
) -> tuple[bool, str]:
    if df is None or df.empty:
        return False, "empty"
    n = len(df)
    if n < min_rows:
        return False, f"rows_{n}_lt_{min_rows}"
    date_col = next((c for c in ("tradingDate", "tradingdate", "time") if c in df.columns), None)
    if date_col is None:
        return False, "no_date_column"
    dt = pd.to_datetime(df[date_col], errors="coerce")
    if dt.isna().all():
        return False, "dates_all_invalid"
    if "close" not in df.columns:
        return False, "no_close"
    close = pd.to_numeric(df["close"], errors="coerce")
    if close.isna().all():
        return False, "close_all_nan"
    if "high" in df.columns and "low" in df.columns:
        high = pd.to_numeric(df["high"], errors="coerce")
        low = pd.to_numeric(df["low"], errors="coerce")
        both = high.notna() & low.notna()
        if both.any():
            err_ratio = float(((high < low) & both).sum() / max(int(both.sum()), 1))
            if err_ratio > 0.05:
                return False, f"high_lt_low_ratio_{err_ratio:.3f}"
    return True, "ok"


def log_ohlcv_quality(label: str, df: pd.DataFrame, src_used: str | None) -> None:
    n = len(df)
    date_col = next((c for c in ("trading_date", "tradingDate", "tradingdate") if c in df.columns), None)
    if date_col:
        dt = pd.to_datetime(df[date_col], errors="coerce")
        dmin, dmax = dt.min(), dt.max()
        dmin_s = dmin.isoformat()[:10] if pd.notna(dmin) else "NA"
        dmax_s = dmax.isoformat()[:10] if pd.notna(dmax) else "NA"
    else:
        dmin_s = dmax_s = "NA"
    pct_close = float(df["close"].isna().mean() * 100) if "close" in df.columns and n else 100.0
    pct_val = float(df["value"].isna().mean() * 100) if "value" in df.columns and n else 100.0
    LOGGER.info(
        "%s | src_used=%s rows=%s min_date=%s max_date=%s pct_missing_close=%.2f%% pct_missing_value=%.2f%%",
        label,
        (src_used or "none").upper(),
        n,
        dmin_s,
        dmax_s,
        pct_close,
        pct_val,
    )


def apply_value_derivation(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    close = pd.to_numeric(out["close"], errors="coerce")
    vol = pd.to_numeric(out["volume"], errors="coerce")
    val = pd.to_numeric(out["value"], errors="coerce")
    derived = close * vol
    need = val.isna()
    out["value"] = val.where(~need, derived)
    out["value_is_derived"] = need.fillna(True).astype(bool)
    out["value"] = pd.to_numeric(out["value"], errors="coerce")
    return out


def build_price_like_schema(
    df: pd.DataFrame,
    ticker: str,
    run_date: str,
    source: str,
    instrument_type: str,
) -> pd.DataFrame:
    out = df.copy()
    if "ticker" not in out.columns:
        out.insert(0, "ticker", ticker.upper().strip())
    else:
        out["ticker"] = out["ticker"].astype(str).str.strip().str.upper()
    out["ingested_at"] = run_date
    out["fetched_at"] = datetime.now(timezone.utc).isoformat()
    out["source"] = (source or "unknown").lower()
    out["instrument_type"] = instrument_type
    for col in ["open", "high", "low", "close", "volume", "value"]:
        if col not in out.columns:
            out[col] = pd.NA
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out = apply_value_derivation(out)
    if "tradingDate" in out.columns:
        out["trading_date"] = pd.to_datetime(
            out["tradingDate"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")
    elif "tradingdate" in out.columns:
        out["trading_date"] = pd.to_datetime(
            out["tradingdate"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")
    if "is_suspicious" not in out.columns:
        hl_bad = out["high"].notna() & out["low"].notna() & (out["high"] < out["low"])
        out["is_suspicious"] = (out["close"] < 0) | hl_bad
    for redundant in ("tradingDate", "tradingdate", "time"):
        if redundant in out.columns and "trading_date" in out.columns:
            out = out.drop(columns=[redundant], errors="ignore")
    priority = [
        "ticker",
        "trading_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "value",
        "value_is_derived",
        "source",
        "instrument_type",
        "ingested_at",
        "fetched_at",
        "is_suspicious",
    ]
    first = [c for c in priority if c in out.columns]
    rest = [c for c in out.columns if c not in first]
    out = out[first + rest]
    LOGGER.info("%s — cột output: %s", ticker, out.columns.tolist())
    return out


def save_partition_parquet(
    df: pd.DataFrame,
    base_dir: Path,
    category: str,
    run_date: str,
    filename: str,
) -> Path:
    path = base_dir / category / f"date={run_date}"
    path.mkdir(parents=True, exist_ok=True)
    out_file = path / f"{filename.upper()}.parquet"
    try:
        df.to_parquet(out_file, engine="pyarrow", index=False)
    except ImportError:
        df.to_parquet(out_file, engine="fastparquet", index=False)
    LOGGER.info("Saved %s rows to %s", len(df), out_file)
    return out_file


def save_master_parquet(df: pd.DataFrame, out_file: Path, append: bool = False) -> Path:
    out_file.parent.mkdir(parents=True, exist_ok=True)
    if append and out_file.exists():
        old_df = pd.read_parquet(out_file)
        df = pd.concat([old_df, df], ignore_index=True)
    try:
        df.to_parquet(out_file, engine="pyarrow", index=False)
    except ImportError:
        df.to_parquet(out_file, engine="fastparquet", index=False)
    LOGGER.info("Saved %s rows to %s", len(df), out_file)
    return out_file
