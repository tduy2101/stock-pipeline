"""Shared ingestion helpers for all ingestion groups (structure, news, bctc)."""

from __future__ import annotations

import logging
import random
import time
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

__all__ = [
    "call_with_retry",
    "configure_logging",
    "load_dotenv_from_project_root",
    "project_root",
    "register_vnstock_api_key_from_env",
    "save_master_parquet",
    "save_partition_parquet",
    "wait_for_rate_limit",
]
