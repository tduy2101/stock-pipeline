from __future__ import annotations

import inspect
import logging
import time
from datetime import datetime, timezone

import pandas as pd
from vnstock import Company, Finance, Listing, Trading

from .common import call_with_retry, save_partition_parquet, wait_for_rate_limit
from .config import IngestionConfig

LOGGER = logging.getLogger(__name__)


def _call_symbols_by_exchange(
    listing_obj: object, cfg: IngestionConfig
) -> pd.DataFrame | None:
    """Call Listing.symbols_by_exchange with version-compatible kwargs."""
    if not hasattr(listing_obj, "symbols_by_exchange"):
        return None
    try:
        wait_for_rate_limit(cfg.rate_limit_rpm)
        sig = inspect.signature(listing_obj.symbols_by_exchange)
        kwargs: dict = {}
        if "lang" in sig.parameters:
            kwargs["lang"] = "vi"
        if "get_all" in sig.parameters:
            kwargs["get_all"] = False
        if "show_log" in sig.parameters:
            kwargs["show_log"] = False
        raw = listing_obj.symbols_by_exchange(**kwargs)
    except Exception as ex:
        LOGGER.warning("symbols_by_exchange failed: %s", ex)
        return None
    if raw is None or not isinstance(raw, pd.DataFrame) or raw.empty:
        return None
    return raw


def _extract_company_row(raw: object) -> dict:
    if isinstance(raw, pd.DataFrame):
        return raw.iloc[0].to_dict() if not raw.empty else {}
    if isinstance(raw, dict):
        return raw.copy()
    return {}


def _source_order_kbs_first(cfg: IngestionConfig) -> list[str]:
    sources = cfg.resolved_data_sources()

    def _priority(src: str) -> int:
        key = (src or "").strip().lower()
        if key == "kbs":
            return 0
        if key == "vci":
            return 2
        return 1

    return sorted(sources, key=_priority)


def _company_source_order(cfg: IngestionConfig) -> list[str]:
    return _source_order_kbs_first(cfg)


def _finance_source_order(cfg: IngestionConfig) -> list[str]:
    return _source_order_kbs_first(cfg)


def _is_incompatible_company_source_error(ex: Exception) -> bool:
    if type(ex).__name__ != "KeyError":
        return False
    msg = str(ex).strip().lower()
    return msg in {"'data'", '"data"', "data"}


def _is_incompatible_finance_source_error(ex: Exception) -> bool:
    if type(ex).__name__ != "KeyError":
        return False
    msg = str(ex).strip().lower()
    return msg in {"'data'", '"data"', "data"}


def _is_transient_finance_source_error(ex: Exception) -> bool:
    name = type(ex).__name__.strip().lower()
    msg = str(ex).strip().lower()
    if "retryerror" in name or "connectionerror" in name or "timeout" in name:
        return True
    transient_tokens = (
        "connectionerror",
        "max retries exceeded",
        "timed out",
        "timeout",
        "temporary failure",
        "name resolution",
        "503",
        "502",
        "504",
        "404",
    )
    return any(tok in msg for tok in transient_tokens)


def _flatten_multiindex_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if isinstance(out.columns, pd.MultiIndex):
        out.columns = [
            "_".join([str(x) for x in c if str(x) != ""]).strip("_")
            for c in out.columns
        ]
    return out


def ingest_listing(cfg: IngestionConfig | None = None) -> str:
    """Write a raw-ish listing snapshot plus ingest metadata.

    Silver is responsible for stock filtering, symbol normalization, exchange
    cleanup, duplicate handling and the analytics-ready column order.
    """
    cfg = cfg or IngestionConfig()
    df = pd.DataFrame()
    src_used = ""
    for src in cfg.resolved_data_sources():
        listing_obj = Listing(source=src)
        try:

            def _by_exchange() -> pd.DataFrame | None:
                return _call_symbols_by_exchange(listing_obj, cfg)

            ex_fetched = call_with_retry(
                _by_exchange,
                max_attempts=cfg.api_retry_max_attempts,
                base_delay_sec=cfg.api_retry_base_delay_sec,
                label=f"symbols_by_exchange@{src}",
            )
            if ex_fetched is not None and not ex_fetched.empty:
                df = ex_fetched.copy()
                src_used = src
                LOGGER.info("symbols_by_exchange: %s rows from %s", len(df), src.upper())
                break

            def _symbols() -> pd.DataFrame:
                wait_for_rate_limit(cfg.rate_limit_rpm)
                return Listing(source=src).all_symbols()

            fetched = call_with_retry(
                _symbols,
                max_attempts=cfg.api_retry_max_attempts,
                base_delay_sec=cfg.api_retry_base_delay_sec,
                label=f"listing@{src}",
            )
            if fetched is not None and not fetched.empty:
                df = fetched.copy()
                src_used = src
                LOGGER.info("all_symbols: %s rows from %s", len(df), src.upper())
                break
        except Exception as ex:
            LOGGER.warning("listing(%s) failed: %s", src, ex)
    if df.empty:
        LOGGER.warning("listing response is empty")
        return ""

    df = _flatten_multiindex_columns(df)
    df["crawled_at"] = cfg.run_date
    df["source"] = src_used
    out_file = cfg.data_lake_root / "listing" / "master" / "listing.parquet"
    out_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(out_file, engine="pyarrow", index=False)
    except ImportError:
        df.to_parquet(out_file, engine="fastparquet", index=False)
    LOGGER.info("Saved raw-ish listing snapshot: %s rows -> %s", len(df), out_file)
    return str(out_file)


def _fetch_company_from_source(
    symbol: str,
    src: str,
    cfg: IngestionConfig,
) -> tuple[dict, str]:
    last_error: Exception | None = None
    for method_name in ("overview", "profile"):

        def _fetch() -> object:
            wait_for_rate_limit(cfg.rate_limit_rpm)
            company = Company(source=src, symbol=symbol)
            if not hasattr(company, method_name):
                raise AttributeError(f"Company[{src}] has no method `{method_name}`")
            return getattr(company, method_name)()

        try:
            raw = call_with_retry(
                _fetch,
                max_attempts=cfg.api_retry_max_attempts,
                base_delay_sec=cfg.api_retry_base_delay_sec,
                label=f"company {symbol}@{src}/{method_name}",
            )
        except AttributeError:
            continue
        except Exception as ex:
            last_error = ex
            if _is_incompatible_company_source_error(ex):
                break
            continue

        row = _extract_company_row(raw)
        if row:
            return row, method_name

    if last_error is not None:
        raise last_error
    return {}, ""


def ingest_company_overview(cfg: IngestionConfig | None = None) -> str:
    """Write a raw-ish company snapshot plus ingest metadata.

    Silver is responsible for text cleanup, date/numeric coercion, ticker
    normalization and current-record dedupe.
    """
    cfg = cfg or IngestionConfig()
    out_file = (
        cfg.data_lake_root
        / "company"
        / "snapshots"
        / f"snapshot_date={cfg.run_date}"
        / "company_overview.parquet"
    )

    symbols_to_fetch = [str(t).strip() for t in cfg.tickers[: cfg.max_tickers_per_run] if str(t).strip()]
    if not symbols_to_fetch:
        LOGGER.info("Company snapshot: no valid symbols in batch.")
        return str(out_file) if out_file.exists() else ""

    rows: list[dict] = []
    n_run = len(symbols_to_fetch)
    company_sources = _company_source_order(cfg)
    disabled_sources: set[str] = set()
    if company_sources != cfg.resolved_data_sources():
        LOGGER.info("Company source order: %s", " -> ".join(company_sources))

    for idx, symbol in enumerate(symbols_to_fetch):
        LOGGER.info("[%s/%s] Fetching company %s...", idx + 1, n_run, symbol)
        row: dict = {}
        src_used = ""
        method_used = ""
        for src in company_sources:
            if src in disabled_sources:
                continue
            try:
                row, method_used = _fetch_company_from_source(symbol, src, cfg)
                if row:
                    src_used = src
                    break
            except Exception as ex:
                LOGGER.warning("Company %s (%s): %s", symbol, src, ex)
                if _is_incompatible_company_source_error(ex):
                    disabled_sources.add(src)
                    LOGGER.warning("Company source %s disabled for this run.", src)
        if not src_used:
            LOGGER.warning("Company %s: no usable source", symbol)
            if idx < n_run - 1:
                time.sleep(cfg.inter_request_delay_sec)
            continue
        row["ticker"] = symbol
        row["source"] = src_used
        if method_used:
            row["company_method"] = method_used
        rows.append(row)
        if idx < n_run - 1:
            time.sleep(cfg.inter_request_delay_sec)

    df_new = pd.DataFrame(rows)
    if df_new.empty:
        LOGGER.warning("company_df is empty; skip save.")
        return str(out_file) if out_file.exists() else ""
    df_new["snapshot_date"] = cfg.run_date
    df_new["fetched_at"] = datetime.now(timezone.utc).isoformat()

    out_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        df_new.to_parquet(out_file, engine="pyarrow", index=False)
    except ImportError:
        df_new.to_parquet(out_file, engine="fastparquet", index=False)
    LOGGER.info(
        "Saved raw-ish company snapshot: rows=%s -> %s",
        len(df_new),
        out_file,
    )
    return str(out_file)


def _fetch_finance_ratio_one(
    symbol: str,
    cfg: IngestionConfig,
    *,
    sources: list[str] | None = None,
    disabled_sources: set[str] | None = None,
) -> tuple[pd.DataFrame, str, str, set[str], bool]:
    source_list = sources or cfg.resolved_data_sources()
    disabled = disabled_sources if disabled_sources is not None else set()
    newly_disabled: set[str] = set()
    attempted_sources = 0
    sources_with_exception = 0
    disable_on_transient_error = bool(
        getattr(cfg, "financial_ratio_disable_source_on_transient_error", True)
    )
    ratio_retry_attempts = max(
        1,
        int(getattr(cfg, "financial_ratio_retry_max_attempts", cfg.api_retry_max_attempts)),
    )
    ratio_retry_base_delay_sec = max(
        0.1,
        float(
            getattr(
                cfg,
                "financial_ratio_retry_base_delay_sec",
                cfg.api_retry_base_delay_sec,
            )
        ),
    )

    for src in source_list:
        if src in disabled:
            continue
        attempted_sources += 1

        source_incompatible = False
        source_had_exception = False
        source_had_transient_error = False
        source_had_non_transient_error = False
        for period in ("quarter", "year"):
            try:

                def _ratio() -> pd.DataFrame:
                    wait_for_rate_limit(cfg.rate_limit_rpm)
                    fin = Finance(source=src, symbol=symbol)
                    return fin.ratio(period=period)

                ratio_df = call_with_retry(
                    _ratio,
                    max_attempts=ratio_retry_attempts,
                    base_delay_sec=ratio_retry_base_delay_sec,
                    label=f"ratio {symbol}@{src}/{period}",
                )
            except Exception as ex:
                source_had_exception = True
                LOGGER.warning("Finance ratio %s (%s, %s): %s", symbol, src, period, ex)
                if _is_incompatible_finance_source_error(ex):
                    source_incompatible = True
                    break
                if _is_transient_finance_source_error(ex):
                    source_had_transient_error = True
                    if disable_on_transient_error:
                        break
                else:
                    source_had_non_transient_error = True
                continue
            if ratio_df is not None and not ratio_df.empty:
                return ratio_df.copy(), src, period, newly_disabled, False

        if source_had_exception:
            sources_with_exception += 1

        if source_incompatible:
            newly_disabled.add(src)
            LOGGER.warning("Finance source %s disabled for this run.", src)
        elif (
            disable_on_transient_error
            and source_had_transient_error
            and not source_had_non_transient_error
        ):
            newly_disabled.add(src)
            LOGGER.warning("Finance source %s disabled after transient errors.", src)

    all_sources_errored = attempted_sources > 0 and sources_with_exception == attempted_sources
    return pd.DataFrame(), "", "", newly_disabled, all_sources_errored


def ingest_financial_ratio(cfg: IngestionConfig | None = None) -> dict[str, str]:
    cfg = cfg or IngestionConfig()
    outputs: dict[str, str] = {}
    n_run = min(len(cfg.tickers), cfg.max_tickers_per_run)
    source_order = _finance_source_order(cfg)
    disabled_sources: set[str] = set()
    consecutive_source_error_tickers = 0
    abort_after_source_errors = max(
        0,
        int(getattr(cfg, "financial_ratio_abort_after_consecutive_source_errors", 2)),
    )
    if source_order != cfg.resolved_data_sources():
        LOGGER.info("Finance source order: %s", " -> ".join(source_order))

    for idx, symbol in enumerate(cfg.tickers[: cfg.max_tickers_per_run]):
        active_sources = [src for src in source_order if src not in disabled_sources]
        if not active_sources:
            LOGGER.warning("Stop financial_ratio early: no active sources remain.")
            break

        LOGGER.info("[%s/%s] Fetching financial ratio %s...", idx + 1, n_run, symbol)
        ratio_df, src_used, period_used, newly_disabled, all_sources_errored = (
            _fetch_finance_ratio_one(
                symbol,
                cfg,
                sources=active_sources,
                disabled_sources=disabled_sources,
            )
        )
        if newly_disabled:
            disabled_sources.update(newly_disabled)
        if ratio_df.empty:
            if all_sources_errored:
                consecutive_source_error_tickers += 1
            else:
                consecutive_source_error_tickers = 0
            LOGGER.warning("Skip %s: empty financial_ratio from all sources/periods", symbol)
            if (
                abort_after_source_errors > 0
                and consecutive_source_error_tickers >= abort_after_source_errors
            ):
                LOGGER.error(
                    "Stop financial_ratio after %s consecutive source-error tickers.",
                    consecutive_source_error_tickers,
                )
                break
            if idx < n_run - 1:
                time.sleep(cfg.inter_request_delay_sec)
            continue
        consecutive_source_error_tickers = 0
        ratio_df["ticker"] = symbol
        ratio_df["ingested_at"] = cfg.run_date
        ratio_df["data_source"] = src_used
        ratio_df["ratio_period"] = period_used
        out_file = save_partition_parquet(
            ratio_df,
            cfg.data_lake_root,
            "financial_ratio",
            cfg.run_date,
            symbol,
            partition_key="snapshot_date",
        )
        outputs[symbol] = str(out_file)
        LOGGER.info(
            "Saved ratio %s: %s rows (src=%s, period=%s)",
            symbol,
            len(ratio_df),
            src_used,
            period_used,
        )
        if idx < n_run - 1:
            time.sleep(cfg.inter_request_delay_sec)
    return outputs


def ingest_price_board(cfg: IngestionConfig | None = None) -> str:
    cfg = cfg or IngestionConfig()
    tickers = [t.upper().strip() for t in cfg.tickers[: cfg.max_tickers_per_run]]
    if not tickers:
        return ""
    raw = None
    src_used = ""
    for src in cfg.resolved_data_sources():
        try:

            def _board() -> object:
                wait_for_rate_limit(cfg.rate_limit_rpm)
                trading = Trading(source=src, symbol=tickers[0])
                return trading.price_board(symbols_list=tickers)

            raw = call_with_retry(
                _board,
                max_attempts=cfg.api_retry_max_attempts,
                base_delay_sec=cfg.api_retry_base_delay_sec,
                label=f"price_board@{src}",
            )
            if raw is not None and not (hasattr(raw, "empty") and raw.empty):
                src_used = src
                break
        except Exception as ex:
            LOGGER.warning("price_board (%s): %s", src, ex)
    if raw is None or (hasattr(raw, "empty") and raw.empty):
        LOGGER.warning("price_board_snapshot is empty from all sources; skip save.")
        return ""
    if not isinstance(raw, pd.DataFrame):
        raw = pd.DataFrame(raw)
    df = _flatten_multiindex_columns(raw)
    snapshot_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
    df["snapshot_at"] = snapshot_at
    df["data_source"] = src_used
    out_file = save_partition_parquet(
        df,
        cfg.data_lake_root,
        "price_board",
        cfg.run_date,
        "price_board_snapshot",
        partition_key="snapshot_at",
        partition_value=snapshot_at,
    )
    return str(out_file)


def ingest_all_stock_info(cfg: IngestionConfig | None = None) -> dict[str, object]:
    cfg = cfg or IngestionConfig()
    return {
        "listing": ingest_listing(cfg),
        "company": ingest_company_overview(cfg),
        "financial_ratio": ingest_financial_ratio(cfg),
        "price_board": ingest_price_board(cfg),
    }
