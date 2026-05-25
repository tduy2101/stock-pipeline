from __future__ import annotations

import json
import logging
import os
import random
import re
import time
from datetime import date, datetime, timedelta, timezone
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
    if isinstance(exc, (ConnectionError, TimeoutError, OSError)):
        return True
    name = type(exc).__name__
    if name in ("RetryError", "ReadTimeout", "ConnectTimeout", "ChunkedEncodingError"):
        return True
    msg = str(exc).lower()
    return "connection" in msg or "timeout" in msg or "temporar" in msg


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
    return Path(__file__).resolve().parents[2]


def load_dotenv_from_project_root() -> bool:
    try:
        from dotenv import load_dotenv
    except ImportError:
        LOGGER.debug("python-dotenv is not installed; skip .env loading")
        return False
    env_path = project_root() / ".env"
    if not env_path.is_file():
        LOGGER.debug("No .env found at %s", env_path)
        return False
    load_dotenv(env_path)
    LOGGER.info("Loaded environment variables from %s", env_path)
    return True


def register_vnstock_api_key_from_env(env_var: str = "VNSTOCK_API_KEY") -> bool:
    import os

    load_dotenv_from_project_root()
    key = os.environ.get(env_var, "").strip()
    if not key:
        LOGGER.info("Environment variable %s is empty; skip register_user.", env_var)
        return False
    try:
        from vnstock import register_user

        register_user(key)
        LOGGER.info("Called register_user from %s.", env_var)
        return True
    except Exception as ex:
        LOGGER.warning("register_user failed: %s", ex)
        return False


def _column_lookup(df: pd.DataFrame) -> dict[str, object]:
    return {
        str(c).strip().lower().replace("_", "").replace(" ", ""): c
        for c in df.columns
    }


def find_trading_date_column(df: pd.DataFrame) -> object | None:
    lookup = _column_lookup(df)
    return next(
        (lookup[key] for key in ("tradingdate", "time", "date") if key in lookup),
        None,
    )


def attach_trading_date_column(df: pd.DataFrame) -> pd.DataFrame:
    date_col = find_trading_date_column(df)
    if date_col is None:
        raise ValueError("missing trading_date/time/date column for trading partition")

    out = df.copy()
    parsed = pd.to_datetime(out[date_col], errors="coerce").dt.normalize()
    valid = parsed.notna()
    if not valid.all():
        LOGGER.warning(
            "Drop %s rows without valid trading_date before partition write",
            int((~valid).sum()),
        )
    out = out.loc[valid].copy()
    out["trading_date"] = parsed.loc[valid].dt.strftime("%Y-%m-%d")
    return out


def iter_trading_date_frames(df: pd.DataFrame):
    out = attach_trading_date_column(df)
    for trading_date, frame in out.groupby("trading_date", sort=True, dropna=True):
        yield str(trading_date), frame.reset_index(drop=True)


def iter_trading_month_frames(df: pd.DataFrame):
    out = attach_trading_date_column(df)
    dt = pd.to_datetime(out["trading_date"], errors="coerce")
    out["_partition_year"] = dt.dt.strftime("%Y")
    out["_partition_month"] = dt.dt.strftime("%m")
    group_cols = ["_partition_year", "_partition_month"]
    for (year, month), frame in out.groupby(group_cols, sort=True, dropna=True):
        cleaned = frame.drop(columns=group_cols).reset_index(drop=True)
        yield str(year), str(month), cleaned


def parse_date_text(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date().isoformat()


def next_date_text(value: str) -> str:
    return (date.fromisoformat(value) + timedelta(days=1)).isoformat()


def partition_value_from_path(path: Path, key: str = "trading_date") -> str | None:
    prefix = f"{key}="
    for part in path.parts:
        if part.startswith(prefix):
            return part[len(prefix) :]
    return None


def month_partition_value_from_path(path: Path) -> str | None:
    year = partition_value_from_path(path, "year")
    month = partition_value_from_path(path, "month")
    if not year or not month:
        return None
    return f"{year}-{month}"


def max_trading_date_in_frame(df: pd.DataFrame) -> str | None:
    if df is None or df.empty:
        return None
    try:
        out = attach_trading_date_column(df)
    except ValueError:
        return None
    values = pd.to_datetime(out["trading_date"], errors="coerce")
    value = values.max()
    if pd.isna(value):
        return None
    return pd.Timestamp(value).date().isoformat()


def transform_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """Compatibility shim; Silver owns analytics-ready OHLCV normalization."""
    return df.copy()


def validate_ohlcv_frame(
    df: pd.DataFrame, *, min_rows: int = 200
) -> tuple[bool, str]:
    """Coarse ingest gate used only for retry/fallback source selection."""
    if df is None or df.empty:
        return False, "empty"
    n = len(df)
    if n < min_rows:
        return False, f"rows_{n}_lt_{min_rows}"
    lookup = _column_lookup(df)
    date_col = next(
        (lookup[key] for key in ("tradingdate", "time", "date") if key in lookup),
        None,
    )
    if date_col is None:
        return False, "no_date_column"
    dt = pd.to_datetime(df[date_col], errors="coerce")
    if dt.isna().all():
        return False, "dates_all_invalid"
    close_col = lookup.get("close")
    if close_col is None:
        return False, "no_close"
    close = pd.to_numeric(df[close_col], errors="coerce")
    if close.isna().all():
        return False, "close_all_nan"
    high_col = lookup.get("high")
    low_col = lookup.get("low")
    if high_col is not None and low_col is not None:
        high = pd.to_numeric(df[high_col], errors="coerce")
        low = pd.to_numeric(df[low_col], errors="coerce")
        both = high.notna() & low.notna()
        if both.any():
            err_ratio = float(((high < low) & both).sum() / max(int(both.sum()), 1))
            if err_ratio > 0.05:
                return False, f"high_lt_low_ratio_{err_ratio:.3f}"
    return True, "ok"


def log_ohlcv_quality(label: str, df: pd.DataFrame, src_used: str | None) -> None:
    lookup = _column_lookup(df)
    n = len(df)
    date_col = next(
        (lookup[key] for key in ("tradingdate", "time", "date") if key in lookup),
        None,
    )
    if date_col is not None:
        dt = pd.to_datetime(df[date_col], errors="coerce")
        dmin, dmax = dt.min(), dt.max()
        dmin_s = dmin.isoformat()[:10] if pd.notna(dmin) else "NA"
        dmax_s = dmax.isoformat()[:10] if pd.notna(dmax) else "NA"
    else:
        dmin_s = dmax_s = "NA"
    close_col = lookup.get("close")
    value_col = lookup.get("value")
    pct_close = float(df[close_col].isna().mean() * 100) if close_col and n else 100.0
    pct_val = float(df[value_col].isna().mean() * 100) if value_col and n else 100.0
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
    """Deprecated for Bronze writes; value derivation is a Silver transform."""
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
    """Attach minimal Bronze metadata to an OHLCV source payload."""
    out = df.copy()
    if "ticker" not in out.columns:
        out["ticker"] = ticker
    out["ingested_at"] = run_date
    out["fetched_at"] = datetime.now(timezone.utc).isoformat()
    out["source"] = (source or "unknown").lower()
    out["instrument_type"] = instrument_type
    LOGGER.info("%s Bronze columns: %s", ticker, out.columns.tolist())
    return out


def save_partition_parquet(
    df: pd.DataFrame,
    base_dir: Path,
    category: str,
    run_date: str,
    filename: str,
    *,
    partition_key: str = "date",
    partition_value: str | None = None,
    partition_parts: dict[str, str] | None = None,
) -> Path:
    path = base_dir / category
    if partition_parts:
        for key, value in partition_parts.items():
            path = path / f"{str(key).strip()}={str(value).strip()}"
    else:
        key = str(partition_key).strip() or "date"
        value = str(partition_value or run_date).strip()
        path = path / f"{key}={value}"
    path.mkdir(parents=True, exist_ok=True)
    out_file = path / f"{filename.upper()}.parquet"
    try:
        df.to_parquet(out_file, engine="pyarrow", index=False)
    except ImportError:
        df.to_parquet(out_file, engine="fastparquet", index=False)
    LOGGER.info("Saved %s rows to %s", len(df), out_file)
    return out_file


def _dedupe_price_like_month_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = attach_trading_date_column(df)
    sort_cols = [c for c in ("ticker", "trading_date", "fetched_at") if c in out.columns]
    if sort_cols:
        out = out.sort_values(sort_cols, kind="stable")
    key_cols = [c for c in ("ticker", "trading_date") if c in out.columns]
    if not key_cols:
        key_cols = ["trading_date"]
    return out.drop_duplicates(key_cols, keep="last").reset_index(drop=True)


def save_monthly_ticker_parquets(
    df: pd.DataFrame,
    base_dir: Path,
    category: str,
    run_date: str,
    filename: str,
    *,
    merge_existing: bool = True,
) -> list[Path]:
    paths: list[Path] = []
    for year, month, frame in iter_trading_month_frames(df):
        out_frame = frame
        out_file = (
            base_dir
            / category
            / f"year={year}"
            / f"month={month}"
            / f"{filename.upper()}.parquet"
        )
        if merge_existing and out_file.exists():
            try:
                existing = pd.read_parquet(out_file)
                out_frame = pd.concat([existing, frame], ignore_index=True, sort=False)
                out_frame = _dedupe_price_like_month_frame(out_frame)
            except Exception as ex:
                LOGGER.warning("Cannot merge existing monthly file %s: %s", out_file, ex)
        paths.append(
            save_partition_parquet(
                out_frame,
                base_dir,
                category,
                run_date,
                filename,
                partition_parts={"year": year, "month": month},
            )
        )
    return paths


def save_trading_date_partition_parquets(
    df: pd.DataFrame,
    base_dir: Path,
    category: str,
    run_date: str,
    filename: str,
) -> list[Path]:
    paths: list[Path] = []
    for trading_date, frame in iter_trading_date_frames(df):
        paths.append(
            save_partition_parquet(
                frame,
                base_dir,
                category,
                run_date,
                filename,
                partition_key="trading_date",
                partition_value=trading_date,
            )
        )
    return paths


def max_trading_date_from_partition_dir(
    dataset_dir: Path,
    *,
    partition_key: str = "trading_date",
) -> str | None:
    if not dataset_dir.exists():
        return None
    values = [
        parse_date_text(p.name.split("=", 1)[1])
        for p in dataset_dir.iterdir()
        if p.is_dir() and p.name.startswith(f"{partition_key}=")
    ]
    valid = [v for v in values if v]
    return max(valid) if valid else None


def _gold_database_url() -> str:
    load_dotenv_from_project_root()
    for env_var in (
        "GOLD_DATABASE_URL",
        "DATABASE_URL",
        "POSTGRES_DSN",
        "POSTGRES_URL",
    ):
        value = os.environ.get(env_var, "").strip()
        if value:
            return value
    return ""


def read_gold_trading_date_watermark(table_names: tuple[str, ...]) -> str | None:
    dsn = _gold_database_url()
    if not dsn:
        return None
    try:
        from sqlalchemy import create_engine, text
    except ImportError:
        LOGGER.warning("SQLAlchemy is not installed; skip Gold watermark")
        return None

    engine = create_engine(dsn)
    found: list[str] = []
    try:
        with engine.connect() as conn:
            for table_name in table_names:
                if not re.match(
                    r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$",
                    table_name,
                ):
                    LOGGER.warning(
                        "Skip unsafe Gold watermark table name: %s",
                        table_name,
                    )
                    continue
                try:
                    result = conn.execute(
                        text(f"SELECT max(trading_date)::text FROM {table_name}")
                    ).scalar()
                except Exception as ex:
                    LOGGER.debug(
                        "Gold watermark unavailable from %s: %s",
                        table_name,
                        ex,
                    )
                    continue
                parsed = parse_date_text(result)
                if parsed:
                    found.append(parsed)
    except Exception as ex:
        LOGGER.warning("Cannot read Gold watermark: %s", ex)
        return None
    finally:
        engine.dispose()
    return max(found) if found else None


def read_raw_watermark(base_dir: Path, dataset: str) -> str | None:
    path = base_dir / "_watermark.json"
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as ex:
        LOGGER.warning("Cannot read watermark file %s: %s", path, ex)
        return None
    entry = payload.get(dataset, {})
    if not isinstance(entry, dict):
        return None
    return parse_date_text(entry.get("last_trading_date") or entry.get("max_trading_date"))


def write_raw_watermark(
    base_dir: Path,
    dataset: str,
    max_trading_date: str,
    *,
    run_id: str,
) -> Path:
    path = base_dir / "_watermark.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_file():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                payload = {}
        except Exception:
            payload = {}
    else:
        payload = {}

    existing = read_raw_watermark(base_dir, dataset)
    candidates = [d for d in (existing, parse_date_text(max_trading_date)) if d]
    if not candidates:
        raise ValueError(f"Cannot write empty trading-date watermark for {dataset}")
    chosen = max(candidates)
    payload[dataset] = {
        "last_trading_date": chosen,
        "max_trading_date": chosen,
        "run_id": run_id,
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    LOGGER.info("Updated raw watermark %s -> %s", dataset, path)
    return path


def resolve_trading_date_watermark(
    *,
    raw_root: Path,
    dataset: str,
    silver_dataset: str,
    gold_tables: tuple[str, ...] = (),
) -> str | None:
    data_lake_root = raw_root.parent.parent
    silver_watermark = max_trading_date_from_partition_dir(
        data_lake_root / "silver" / silver_dataset
    )
    gold_watermark = (
        read_gold_trading_date_watermark(gold_tables) if gold_tables else None
    )
    raw_watermark = read_raw_watermark(raw_root, dataset)
    candidates = [d for d in (gold_watermark, silver_watermark, raw_watermark) if d]
    return max(candidates) if candidates else None


def collect_partition_output_stats(paths: list[str] | list[Path]) -> dict[str, object]:
    trading_dates: list[str] = []
    row_count = 0
    for raw_path in paths:
        path = Path(raw_path)
        try:
            df = pd.read_parquet(path)
            row_count += len(df)
            try:
                dated = attach_trading_date_column(df)
                trading_dates.extend(dated["trading_date"].dropna().astype(str).tolist())
            except ValueError:
                trading_date = partition_value_from_path(path)
                if trading_date:
                    trading_dates.append(trading_date)
        except Exception as ex:
            LOGGER.warning("Cannot count rows in %s: %s", path, ex)

    valid_dates = [d for d in (parse_date_text(x) for x in trading_dates) if d]
    return {
        "trading_date_from": min(valid_dates) if valid_dates else None,
        "trading_date_to": max(valid_dates) if valid_dates else None,
        "row_count": int(row_count),
    }


def write_run_metadata(
    base_dir: Path,
    category: str,
    *,
    run_id: str,
    run_type: str,
    tickers: list[str],
    paths: list[str] | list[Path],
    requested_count: int | None = None,
) -> Path:
    stats = collect_partition_output_stats(paths)
    status = "success"
    if requested_count is not None and len(tickers) < requested_count:
        status = "partial"
    if not tickers:
        status = "partial"

    payload = {
        "run_id": run_id,
        "run_type": run_type,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "trading_date_from": stats["trading_date_from"],
        "trading_date_to": stats["trading_date_to"],
        "tickers": tickers,
        "row_count": stats["row_count"],
        "status": status,
    }
    path = base_dir / category / "_runs" / f"{run_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    LOGGER.info("Wrote %s run metadata -> %s", category, path)
    return path


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
