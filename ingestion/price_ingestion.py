"""
Production-grade OHLCV price ingestion for Vietnamese equities via vnstock.

Designed for scheduled runs (e.g. Apache Airflow): modular functions, logging,
rate limiting, retries, Hive-style Parquet layout, and PostgreSQL upserts.

Configuration is environment-driven; see ``PipelineSettings.from_env()`` and
``PIPELINE_*`` / ``DATABASE_URL`` variables.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Final

import pandas as pd
import requests
from requests import exceptions as req_exc
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from vnstock import Quote

try:
    from psycopg2.extras import execute_batch
except ImportError:  # pragma: no cover - optional until DB deps installed
    execute_batch = None  # type: ignore[misc, assignment]


logger = logging.getLogger(__name__)

PRIMARY_QUOTE_SOURCE: Final[str] = "kbs"
FALLBACK_QUOTE_SOURCE: Final[str] = "vci"

# vnstock Guest tier: stay under 20 requests / minute.
_REQUESTS_PER_MINUTE_LIMIT: Final[float] = 20.0
_MIN_SECONDS_BETWEEN_REQUESTS: Final[float] = 60.0 / _REQUESTS_PER_MINUTE_LIMIT + 0.1

# Retry: up to 3 attempts, backoff 2s, 4s, 8s (applied before attempt 2, 3, 4).
_BACKOFF_SECONDS: Final[tuple[int, ...]] = (2, 4, 8)
_MAX_FETCH_ATTEMPTS: Final[int] = 3

_LAST_API_MONO: float = 0.0
_API_THROTTLE_LOCK = threading.Lock()


def _sleep_until_next_api_slot() -> None:
    """
    Enforce a minimum interval between outbound vnstock/API calls so the overall
    request rate stays below the Guest limit (~20/min).
    """
    global _LAST_API_MONO
    with _API_THROTTLE_LOCK:
        now = time.monotonic()
        elapsed = now - _LAST_API_MONO
        wait = _MIN_SECONDS_BETWEEN_REQUESTS - elapsed
        if wait > 0:
            time.sleep(wait)
        _LAST_API_MONO = time.monotonic()


def _is_retryable_error(exc: BaseException) -> bool:
    """Return True if the error may succeed on retry (network or common transient HTTP)."""
    if isinstance(exc, (req_exc.Timeout, req_exc.ConnectionError, req_exc.ChunkedEncodingError)):
        return True
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        return exc.response.status_code in {429, 500, 502, 503, 504}
    # vnstock may wrap requests; fall back to status code on generic Exception
    resp = getattr(exc, "response", None)
    code = getattr(resp, "status_code", None)
    if code in {429, 500, 502, 503, 504}:
        return True
    return isinstance(exc, (TimeoutError, OSError))


def _attempt_history(
    source: str,
    symbol: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    """Call vnstock Quote.history for one source (single try, no outer retry)."""
    quote = Quote(source=source, symbol=symbol)
    df = quote.history(start=start_date, end=end_date, interval="1D")
    if df is None:
        return pd.DataFrame()
    return df.copy()


def fetch_price_data(
    ticker: str,
    start_date: str,
    end_date: str,
) -> tuple[pd.DataFrame, str]:
    """
    Download daily OHLCV history for ``ticker`` between ISO dates (inclusive).

    Tries ``kbs`` first, then ``vci``. ``tcbs`` is never used (deprecated in vnstock).

    Applies throttling (<20 req/min), and up to three attempts per source with
    exponential backoff sleeps (2s, 4s, 8s) on retryable failures including HTTP 429.

    Returns:
        A pair ``(dataframe, source_label)``. ``source_label`` is ``"kbs"``, ``"vci"``,
        or ``""`` if both sources failed. The frame may be empty on total failure.
    """
    sym = ticker.upper().strip()
    sources: tuple[str, ...] = (PRIMARY_QUOTE_SOURCE, FALLBACK_QUOTE_SOURCE)

    for src in sources:
        last_err: BaseException | None = None
        for attempt in range(_MAX_FETCH_ATTEMPTS):
            _sleep_until_next_api_slot()
            try:
                df = _attempt_history(src, sym, start_date, end_date)
                if df is not None and not df.empty:
                    df.attrs["fetch_ticker"] = sym
                    df.attrs["vnstock_source"] = src
                    logger.info("Fetched %s rows for %s from source=%s", len(df), sym, src)
                    return df, src
            except BaseException as exc:
                last_err = exc
                if attempt + 1 < _MAX_FETCH_ATTEMPTS and _is_retryable_error(exc):
                    backoff = _BACKOFF_SECONDS[attempt]
                    logger.warning(
                        "Fetch attempt %s/%s failed for %s source=%s: %s — sleeping %ss",
                        attempt + 1,
                        _MAX_FETCH_ATTEMPTS,
                        sym,
                        src,
                        exc,
                        backoff,
                    )
                    time.sleep(backoff)
                else:
                    break

        if last_err is not None:
            logger.warning("Giving up source=%s for %s after errors: %s", src, sym, last_err)

    logger.error("No price data for %s in range %s → %s", sym, start_date, end_date)
    return pd.DataFrame(), ""


def _normalize_column_key(name: str) -> str:
    return str(name).strip().lower().replace(" ", "")


def transform_price_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize a raw vnstock OHLCV frame.

    Expects ``df.attrs`` populated by :func:`fetch_price_data` with ``fetch_ticker`` and
    ``vnstock_source``.

    - Renames date-like columns (``tradingDate``, ``trading_date``, ``time``, …) to ``date``.
    - Casts ``open/high/low/close`` to float, ``volume`` and ``value`` to nullable integer.
    - Scales prices from thousand-VND if values look unscaled (same heuristic as notebook).
    - Sorts by ``date`` ascending and drops duplicates on ``(date, ticker)``.
    - Adds metadata: ``ticker``, ``source``, ``created_at`` (UTC).

    Does **not** set ``is_suspicious``; use ``validate_price_data`` for quality flags.

    Args:
        df: Raw dataframe from ``fetch_price_data``.

    Returns:
        Standardized dataframe; may be empty if input had no valid rows.
    """
    if df.empty:
        return df.copy()

    ticker = str(df.attrs.get("fetch_ticker", "")).upper().strip()
    source = str(df.attrs.get("vnstock_source", ""))
    if not ticker:
        raise ValueError("transform_price_data requires df.attrs['fetch_ticker'] from fetch_price_data")

    out = df.copy()
    rename_map: dict[str, str] = {}
    for col in out.columns:
        key = _normalize_column_key(col)
        if key in {"tradingdate", "trading_date", "date", "time", "ngay"}:
            rename_map[col] = "date"
        else:
            rename_map[col] = key
    out = out.rename(columns=rename_map)

    base_cols = {"date", "open", "high", "low", "close", "volume"}
    missing = base_cols - set(out.columns)
    if missing:
        raise KeyError(f"Missing required columns after rename: {missing}")

    if "value" not in out.columns:
        out["value"] = pd.NA

    keep = ["date", "open", "high", "low", "close", "volume", "value"]
    out = out.loc[:, [c for c in keep if c in out.columns]].copy()

    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize()
    out = out.dropna(subset=["date"])

    for col in ("open", "high", "low", "close"):
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out["volume"] = pd.to_numeric(out["volume"], errors="coerce")
    out["value"] = pd.to_numeric(out["value"], errors="coerce")
    out = out.dropna(subset=["open", "high", "low", "close"])

    ohlc_max = out[["open", "high", "low", "close"]].max(axis=1, numeric_only=True).max()
    if pd.notna(ohlc_max) and float(ohlc_max) > 10_000:
        for col in ("open", "high", "low", "close"):
            out[col] = out[col] / 1000.0

    out["open"] = out["open"].astype("float64")
    out["high"] = out["high"].astype("float64")
    out["low"] = out["low"].astype("float64")
    out["close"] = out["close"].astype("float64")
    out["volume"] = out["volume"].fillna(0).round().astype("Int64")
    out["value"] = out["value"].round().astype("Int64")

    out["ticker"] = ticker
    out["source"] = source if source else ""
    out["created_at"] = pd.Timestamp.now(tz=timezone.utc)

    out = out.sort_values("date", ascending=True)
    out = out.drop_duplicates(subset=["date", "ticker"], keep="last")
    return out.reset_index(drop=True)


def validate_price_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add a boolean ``is_suspicious`` column for rows that violate basic OHLC/volume sanity.

    Marked True when any of: ``high < open``, ``high < close``, ``low > open``,
    ``low > close``, or ``volume < 0``. Rows are never dropped here.
    """
    if df.empty:
        out = df.copy()
        out["is_suspicious"] = pd.Series(dtype="boolean")
        return out

    out = df.copy()
    open_ = pd.to_numeric(out["open"], errors="coerce")
    high = pd.to_numeric(out["high"], errors="coerce")
    low = pd.to_numeric(out["low"], errors="coerce")
    close = pd.to_numeric(out["close"], errors="coerce")
    vol = pd.to_numeric(out["volume"], errors="coerce")

    bad = (
        (high < open_)
        | (high < close)
        | (low > open_)
        | (low > close)
        | (vol < 0)
    )
    out["is_suspicious"] = bad.fillna(False)
    return out


def save_to_parquet(df: pd.DataFrame, ticker: str, data_lake_root: Path | None = None) -> list[Path]:
    """
    Write Hive-partitioned Parquet files:

        ``data-lake/raw/price/ticker={TICKER}/date={YYYY-MM-DD}.parquet``

    One file is written per distinct trading ``date`` present in ``df``.

    Args:
        df: Validated dataframe (must contain column ``date``).
        ticker: Symbol used in the partition path (uppercased).
        data_lake_root: Base ``.../data-lake/raw/price`` directory. Defaults next to repo root.

    Returns:
        List of written file paths.
    """
    if df.empty:
        logger.warning("save_to_parquet skipped for %s: empty frame", ticker)
        return []

    root = data_lake_root or _default_data_lake_price_root()
    safe_ticker = ticker.upper().strip()
    part_dir = root / f"ticker={safe_ticker}"
    part_dir.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []
    dates = pd.to_datetime(df["date"], errors="coerce").dt.date.unique()
    for d in sorted(dates):
        if pd.isna(d):
            continue
        slug = d.isoformat() if isinstance(d, date) else pd.Timestamp(d).date().isoformat()
        sub = df.loc[pd.to_datetime(df["date"]).dt.date == d].copy()
        path = part_dir / f"date={slug}.parquet"
        try:
            sub.to_parquet(path, engine="pyarrow", index=False)
        except ImportError:
            sub.to_parquet(path, engine="fastparquet", index=False)
        written.append(path)
        logger.info("Wrote %s rows → %s", len(sub), path)
    return written


def _default_data_lake_price_root() -> Path:
    """Resolve ``<repo>/data-lake/raw/price`` from this file location."""
    return Path(__file__).resolve().parent.parent / "data-lake" / "raw" / "price"


def _sanitize_sql_identifier(name: str) -> str:
    if not name.isidentifier():
        raise ValueError(f"Unsafe SQL identifier: {name!r}")
    return name


def get_max_trade_date(engine: Engine, ticker: str, table_name: str = "price_daily") -> date | None:
    """
    Return the latest ``date`` stored in PostgreSQL for ``ticker``, or ``None`` if absent.
    """
    sym = ticker.upper().strip()
    table_name = _sanitize_sql_identifier(table_name)
    q = text(f"SELECT MAX(date) AS d FROM {table_name} WHERE ticker = :ticker")
    with engine.connect() as conn:
        row = conn.execute(q, {"ticker": sym}).mappings().first()
    if row is None or row["d"] is None:
        return None
    d = row["d"]
    if isinstance(d, datetime):
        return d.date()
    if hasattr(d, "to_pydatetime"):
        return d.to_pydatetime().date()  # type: ignore[no-any-return]
    return d  # type: ignore[no-any-return]


def _years_ago(d: date, years: int) -> date:
    try:
        return d.replace(year=d.year - years)
    except ValueError:
        return d.replace(year=d.year - years, day=28)


def resolve_incremental_window(
    engine: Engine | None,
    ticker: str,
    end: date,
    *,
    table_name: str = "price_daily",
    initial_lookback_years: int = 3,
) -> tuple[str, str] | None:
    """
    Compute ``(start_iso, end_iso)`` for the next API pull.

    If PostgreSQL has rows for ``ticker``, start at ``MAX(date) + 1``.
    Otherwise start ``initial_lookback_years`` before ``end``.

    Returns:
        ``None`` when the warehouse is already current through ``end`` (no API call needed).
    """
    end_s = end.isoformat()
    if engine is None:
        start_d = _years_ago(end, initial_lookback_years)
        return start_d.isoformat(), end_s

    max_d = get_max_trade_date(engine, ticker, table_name=table_name)
    if max_d is None:
        start_d = _years_ago(end, initial_lookback_years)
    else:
        start_d = max_d + timedelta(days=1)

    if start_d > end:
        return None

    return start_d.isoformat(), end_s


def ensure_price_table(engine: Engine, table_name: str = "price_daily") -> None:
    """
    Create the target table if it does not exist, with a primary key on (ticker, date).
    """
    table_name = _sanitize_sql_identifier(table_name)
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        ticker VARCHAR(32) NOT NULL,
        date DATE NOT NULL,
        open DOUBLE PRECISION NOT NULL,
        high DOUBLE PRECISION NOT NULL,
        low DOUBLE PRECISION NOT NULL,
        close DOUBLE PRECISION NOT NULL,
        volume BIGINT NOT NULL,
        value BIGINT NULL,
        source VARCHAR(16) NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        is_suspicious BOOLEAN NOT NULL,
        PRIMARY KEY (ticker, date)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
    logger.info("Ensured table exists: %s", table_name)


def load_to_postgres(
    df: pd.DataFrame,
    database_url: str | None = None,
    *,
    table_name: str = "price_daily",
    batch_size: int = 750,
) -> int:
    """
    Upsert rows into PostgreSQL using batched inserts (psycopg2 ``execute_batch``).

    Requires ``ON CONFLICT DO UPDATE`` on ``(ticker, date)`` (primary key).
    If ``database_url`` is omitted, reads ``DATABASE_URL`` from the environment.

    Returns:
        Number of rows processed (insert + conflict updates count as processed).
    """
    if execute_batch is None:
        raise ImportError("psycopg2 is required for load_to_postgres; install psycopg2-binary.")

    url = database_url or os.environ.get("DATABASE_URL")
    if not url:
        raise ValueError("database_url or DATABASE_URL environment variable is required")

    if df.empty:
        logger.warning("load_to_postgres: empty dataframe, nothing to load")
        return 0

    table_name = _sanitize_sql_identifier(table_name)
    engine = create_engine(url)
    ensure_price_table(engine, table_name=table_name)

    rows: list[tuple[Any, ...]] = []
    cols = [
        "ticker",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "value",
        "source",
        "created_at",
        "is_suspicious",
    ]
    for _, row in df[cols].iterrows():
        d = row["date"]
        if hasattr(d, "date") and not isinstance(d, date):
            d = pd.Timestamp(d).date()
        elif isinstance(d, pd.Timestamp):
            d = d.date()
        vol = row["volume"]
        vol_i = int(vol) if pd.notna(vol) else 0
        val = row["value"]
        val_i: int | None = int(val) if pd.notna(val) else None
        ca = row["created_at"]
        if isinstance(ca, pd.Timestamp):
            ca = ca.to_pydatetime()
        if getattr(ca, "tzinfo", None) is None:
            ca = ca.replace(tzinfo=timezone.utc)
        rows.append(
            (
                str(row["ticker"]),
                d,
                float(row["open"]),
                float(row["high"]),
                float(row["low"]),
                float(row["close"]),
                vol_i,
                val_i,
                str(row["source"]),
                ca,
                bool(row["is_suspicious"]),
            )
        )

    sql = f"""
    INSERT INTO {table_name} (
        ticker, date, open, high, low, close, volume, value, source, created_at, is_suspicious
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    ON CONFLICT (ticker, date) DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume,
        value = EXCLUDED.value,
        source = EXCLUDED.source,
        created_at = EXCLUDED.created_at,
        is_suspicious = EXCLUDED.is_suspicious;
    """

    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        try:
            execute_batch(cur, sql, rows, page_size=batch_size)
            raw.commit()
        finally:
            cur.close()
    finally:
        raw.close()

    logger.info("Loaded %s rows into %s", len(rows), table_name)
    return len(rows)


@dataclass
class PipelineSettings:
    """
    Runtime configuration for :func:`run_pipeline`.

    Environment variables (optional overrides):

    - ``DATABASE_URL``: SQLAlchemy URL for PostgreSQL.
    - ``PIPELINE_DATA_LAKE_ROOT``: absolute path to ``.../data-lake/raw/price``.
    - ``PIPELINE_TICKERS``: comma-separated list (else built-in default watchlist).
    """

    database_url: str | None = None
    data_lake_root: Path | None = None
    tickers: list[str] = field(
        default_factory=lambda: list(_DEFAULT_TICKERS),
    )
    table_name: str = "price_daily"

    @staticmethod
    def from_env() -> PipelineSettings:
        db = os.environ.get("DATABASE_URL")
        root = os.environ.get("PIPELINE_DATA_LAKE_ROOT")
        tix = os.environ.get("PIPELINE_TICKERS")
        tickers = [x.strip().upper() for x in tix.split(",")] if tix else list(_DEFAULT_TICKERS)
        return PipelineSettings(
            database_url=db,
            data_lake_root=Path(root).resolve() if root else None,
            tickers=tickers,
        )


_DEFAULT_TICKERS: tuple[str, ...] = (
    "ACB",
    "BCM",
    "BID",
    "BVH",
    "CTG",
    "FPT",
    "GAS",
    "GVR",
    "HDB",
    "HPG",
    "MBB",
    "MSN",
    "MWG",
    "PLX",
    "POW",
    "SAB",
    "SHB",
    "SSI",
    "STB",
    "TCB",
    "TPB",
    "VCB",
    "VHM",
    "VIB",
    "VIC",
    "VJC",
    "VNM",
    "VPB",
    "VRE",
    "VGC",
    "DGC",
    "DPM",
    "DCM",
    "HSG",
    "NKG",
    "KDC",
    "QNS",
    "SCS",
    "ACV",
    "VTP",
    "KBC",
    "NVL",
    "PDR",
    "DXG",
    "KDH",
    "AGG",
    "HDG",
    "REE",
    "PNJ",
    "GMD",
)


def run_pipeline(settings: PipelineSettings | None = None) -> None:
    """
    End-to-end ingestion for all configured tickers: fetch → transform → validate
    → Parquet → optional PostgreSQL.

    Skips API calls when incremental window is empty (already up to date).
    """
    cfg = settings or PipelineSettings.from_env()
    lake = cfg.data_lake_root or _default_data_lake_price_root()
    engine: Engine | None = None
    if cfg.database_url:
        engine = create_engine(cfg.database_url)

    today = date.today()
    total = len(cfg.tickers)
    for idx, ticker in enumerate(cfg.tickers):
        logger.info("[%s/%s] Processing %s", idx + 1, total, ticker)
        try:
            window = resolve_incremental_window(
                engine,
                ticker,
                today,
                table_name=cfg.table_name,
            )
            if window is None:
                logger.info("%s up to date through %s — skip", ticker, today.isoformat())
                continue
            start_s, end_s = window

            raw, _ = fetch_price_data(ticker, start_s, end_s)
            if raw.empty:
                logger.warning("No raw data for %s", ticker)
                continue
            cleaned = transform_price_data(raw)
            if cleaned.empty:
                logger.warning("Empty after transform for %s", ticker)
                continue
            checked = validate_price_data(cleaned)
            save_to_parquet(checked, ticker, data_lake_root=lake)
            if cfg.database_url:
                load_to_postgres(checked, cfg.database_url, table_name=cfg.table_name)
        except Exception:
            logger.exception("Failed processing ticker %s", ticker)


if __name__ == "__main__":
    _lvl_name = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, _lvl_name, logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    run_pipeline(PipelineSettings.from_env())
