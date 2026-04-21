from __future__ import annotations

import inspect
import logging
import time
import uuid
from datetime import datetime, timezone

import pandas as pd
from vnstock import Company, Finance, Listing, Trading

from .common import call_with_retry, save_partition_parquet, wait_for_rate_limit
from .config import IngestionConfig

LOGGER = logging.getLogger(__name__)


def _call_symbols_by_exchange(listing_obj: object, cfg: IngestionConfig) -> pd.DataFrame | None:
    """Gọi Listing.symbols_by_exchange với tham số phù hợp KBS/VCI."""
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
        LOGGER.warning("symbols_by_exchange: %s", ex)
        return None
    if raw is None:
        return None
    if hasattr(raw, "empty") and raw.empty:
        return None
    if not isinstance(raw, pd.DataFrame):
        return None
    return raw


def _clean_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    text_cols = out.select_dtypes(include="object").columns
    for col in text_cols:
        out[col] = (
            out[col]
            .astype(str)
            .str.replace("\n", " ", regex=False)
            .str.replace("\r", " ", regex=False)
            .str.strip()
        )
        out.loc[out[col].str.lower().isin(["nan", "none", "null"]), col] = pd.NA
    return out


def _extract_company_row(raw: object) -> dict:
    if isinstance(raw, pd.DataFrame):
        return raw.iloc[0].to_dict() if not raw.empty else {}
    if isinstance(raw, dict):
        return raw.copy()
    return {}


def _company_source_order(cfg: IngestionConfig) -> list[str]:
    """Ưu tiên KBS cho company overview vì VCI có thể lỗi response theo version."""
    sources = cfg.resolved_data_sources()

    def _priority(src: str) -> int:
        key = (src or "").strip().lower()
        if key == "kbs":
            return 0
        if key == "vci":
            return 2
        return 1

    return sorted(sources, key=_priority)


def _is_incompatible_company_source_error(ex: Exception) -> bool:
    if type(ex).__name__ != "KeyError":
        return False
    msg = str(ex).strip().lower()
    return msg in {"'data'", '"data"', "data"}


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
            # Lỗi incompatibility kiểu KeyError('data'): không cần thử method tiếp theo.
            if _is_incompatible_company_source_error(ex):
                break
            continue

        row = _extract_company_row(raw)
        if row:
            return row, method_name

    if last_error is not None:
        raise last_error
    return {}, ""


def _finance_source_order(cfg: IngestionConfig) -> list[str]:
    """Ưu tiên KBS cho financial ratio; VCI có thể lỗi response theo version."""
    sources = cfg.resolved_data_sources()

    def _priority(src: str) -> int:
        key = (src or "").strip().lower()
        if key == "kbs":
            return 0
        if key == "vci":
            return 2
        return 1

    return sorted(sources, key=_priority)


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


def ingest_listing(cfg: IngestionConfig | None = None) -> str:
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
                ex_fetched = ex_fetched.copy()
                ex_fetched.columns = [str(c).strip().lower() for c in ex_fetched.columns]
                if "symbol" in ex_fetched.columns and "exchange" in ex_fetched.columns:
                    df = ex_fetched
                    src_used = src
                    LOGGER.info(
                        "symbols_by_exchange: %s mã (có sàn) từ %s",
                        len(df),
                        src.upper(),
                    )
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
                LOGGER.info("all_symbols: %s mã từ %s", len(df), src.upper())
                break
        except Exception as ex:
            LOGGER.warning("listing(%s) lỗi: %s", src, ex)
    if df.empty:
        LOGGER.warning("Không lấy được listing")
        return ""
    df.columns = [str(c).strip().lower() for c in df.columns]
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    if "exchange" in df.columns:
        ex = df["exchange"].astype(str).str.strip().str.upper()
        df["exchange"] = ex.mask(ex.isin(["", "NAN", "NONE", "<NA>"]))
    exchange_rows: list[dict] = []
    # Theo vnstock KBS: VNMidCap, VNSmallCap — map cả alias cũ để tương thích
    index_exchange_map = {
        "VN30": "HOSE",
        "VNMIDCAP": "HOSE",
        "VNMidCap": "HOSE",
        "VNSML": "HOSE",
        "VNSmallCap": "HOSE",
        "VN100": "HOSE",
        "VNALL": "HOSE",
        "HNX30": "HNX",
        "HNXINDEX": "HNX",
    }
    need_fallback = "exchange" not in df.columns or df["exchange"].isna().all()
    if need_fallback or df["exchange"].isna().any():
        try:
            listing_obj = Listing(source=src_used or cfg.resolved_data_sources()[0])
            for idx_code, exchange in index_exchange_map.items():
                try:
                    if not hasattr(listing_obj, "symbols_by_group"):
                        continue
                    idx_raw = listing_obj.symbols_by_group(group=idx_code)
                    if idx_raw is None:
                        continue
                    if isinstance(idx_raw, pd.Series):
                        if idx_raw.empty:
                            continue
                        syms = idx_raw.dropna().astype(str).str.strip().str.upper()
                    else:
                        idx_df = idx_raw
                        if idx_df.empty:
                            continue
                        idx_df.columns = [str(c).strip().lower() for c in idx_df.columns]
                        sym_col = next(
                            (c for c in ["symbol", "ticker", "code"] if c in idx_df.columns),
                            None,
                        )
                        if not sym_col:
                            continue
                        syms = idx_df[sym_col].dropna().astype(str).str.strip().str.upper()
                    for sym in syms.unique():
                        exchange_rows.append({"symbol": sym, "exchange": exchange})
                except Exception as ex:
                    LOGGER.debug("symbols_by_group(%s) lỗi: %s", idx_code, ex)
        except Exception as ex:
            LOGGER.warning("build exchange map lỗi: %s", ex)
        if exchange_rows:
            emap = pd.DataFrame(exchange_rows)
            prio = {"HOSE": 0, "HNX": 1, "UPCOM": 2}
            emap["_p"] = emap["exchange"].map(prio).fillna(9)
            emap = emap.sort_values("_p").drop_duplicates("symbol", keep="first").drop(
                columns=["_p"]
            )
            df = df.drop(columns=["exchange"], errors="ignore").merge(
                emap, on="symbol", how="left"
            )
        elif "exchange" not in df.columns:
            df["exchange"] = pd.NA
    if "exchange" not in df.columns:
        df["exchange"] = pd.NA
    df["exchange"] = df["exchange"].fillna("UNKNOWN")
    before = len(df)
    if "type" in df.columns:
        tnorm = df["type"].astype(str).str.lower().str.strip()
        df = df[tnorm == "stock"].copy()
    elif "id" in df.columns:
        df = df[df["id"] == 1].copy()
    LOGGER.info("Lọc stock: %s -> %s dòng", before, len(df))
    if "organ_name_x" in df.columns:
        df = df.rename(columns={"organ_name_x": "organ_name"})
    if "organ_name_y" in df.columns:
        df = df.drop(columns=["organ_name_y"])
    if "id" in df.columns:
        df = df.drop(columns=["id"])
    df["crawled_at"] = cfg.run_date
    priority = ["symbol", "organ_name", "en_organ_name", "exchange", "type", "crawled_at"]
    existing = [c for c in priority if c in df.columns]
    other = [c for c in df.columns if c not in existing]
    df = df[existing + other]
    out_file = cfg.data_lake_root / "listing" / "master" / "listing.parquet"
    out_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_parquet(out_file, engine="pyarrow", index=False)
    except ImportError:
        df.to_parquet(out_file, engine="fastparquet", index=False)
    LOGGER.info("✓ Đã ghi %s dòng -> %s", len(df), out_file)
    return str(out_file)


def ingest_company_overview(cfg: IngestionConfig | None = None) -> str:
    cfg = cfg or IngestionConfig()
    out_file = cfg.data_lake_root / "company" / "master" / "company_overview.parquet"
    df_old = pd.DataFrame()
    if out_file.exists():
        try:
            df_old = pd.read_parquet(out_file)
        except Exception:
            LOGGER.exception("Không đọc được file master %s, coi như chưa có mã cũ.", out_file)
            df_old = pd.DataFrame()

    tickers_batch = cfg.tickers[: cfg.max_tickers_per_run]
    symbols_to_fetch = [
        str(t).strip().upper()
        for t in tickers_batch
        if str(t).strip()
    ]
    if not symbols_to_fetch:
        LOGGER.info("Company master: không có ticker hợp lệ trong batch.")
        return str(out_file) if out_file.exists() else ""

    rows: list[dict] = []
    n_run = len(symbols_to_fetch)
    company_sources = _company_source_order(cfg)
    disabled_sources: set[str] = set()
    if company_sources != cfg.resolved_data_sources():
        LOGGER.info("Company source order: %s", " -> ".join(company_sources))

    for idx, symbol in enumerate(symbols_to_fetch):
        LOGGER.info("[%s/%s] Đang tải company %s...", idx + 1, n_run, symbol)
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
                    LOGGER.warning(
                        "Company source %s tạm disable cho phần còn lại của run (lỗi không tương thích response).",
                        src,
                    )
        if not src_used:
            LOGGER.warning("Company %s: không lấy được từ nguồn nào", symbol)
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
        LOGGER.warning("company_df rỗng — bỏ qua lưu.")
        return str(out_file) if out_file.exists() else ""
    df_new = _clean_text_columns(df_new)
    df_new["ticker"] = df_new["ticker"].astype(str).str.strip().str.upper()
    if "source" not in df_new.columns:
        df_new["source"] = cfg.primary_source
    df_new = df_new.drop_duplicates(subset=["ticker", "source"], keep="last")
    df_new["snapshot_date"] = cfg.run_date
    df_new["fetched_at"] = datetime.now(timezone.utc).isoformat()
    df_new["ingest_run_id"] = str(uuid.uuid4())
    out_file.parent.mkdir(parents=True, exist_ok=True)
    if df_old.empty:
        combined = df_new
    else:
        cols = list(dict.fromkeys(list(df_old.columns) + list(df_new.columns)))
        combined = pd.concat(
            [df_old.reindex(columns=cols), df_new.reindex(columns=cols)],
            ignore_index=True,
            sort=False,
        )

    dedupe_keys = [
        c for c in ["ticker", "snapshot_date", "source"] if c in combined.columns
    ]
    if dedupe_keys:
        sort_cols = [c for c in ["fetched_at", "ingest_run_id"] if c in combined.columns]
        if sort_cols:
            combined = combined.sort_values(sort_cols, kind="stable")
        combined = combined.drop_duplicates(subset=dedupe_keys, keep="last")

    try:
        combined.to_parquet(out_file, engine="pyarrow", index=False)
    except ImportError:
        combined.to_parquet(out_file, engine="fastparquet", index=False)
    LOGGER.info(
        "✓ company master: thêm %s snapshot, tổng %s dòng lịch sử -> %s",
        len(df_new),
        len(combined),
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
            LOGGER.warning(
                "Finance source %s tạm disable cho phần còn lại của run (lỗi không tương thích response).",
                src,
            )
        elif (
            disable_on_transient_error
            and source_had_transient_error
            and not source_had_non_transient_error
        ):
            newly_disabled.add(src)
            LOGGER.warning(
                "Finance source %s tạm disable cho phần còn lại của run (lỗi kết nối/retry lặp lại).",
                src,
            )

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
            LOGGER.warning(
                "Dừng financial_ratio sớm: không còn nguồn khả dụng sau khi disable (%s)",
                ", ".join(sorted(disabled_sources)) if disabled_sources else "none",
            )
            break

        LOGGER.info("[%s/%s] Đang tải financial ratio %s...", idx + 1, n_run, symbol)
        ratio_df, src_used, period_used, newly_disabled, all_sources_errored = _fetch_finance_ratio_one(
            symbol,
            cfg,
            sources=active_sources,
            disabled_sources=disabled_sources,
        )
        if newly_disabled:
            disabled_sources.update(newly_disabled)
        if ratio_df.empty:
            if all_sources_errored:
                consecutive_source_error_tickers += 1
            else:
                consecutive_source_error_tickers = 0
            LOGGER.warning("Bỏ qua %s — financial_ratio rỗng mọi nguồn/kỳ", symbol)
            if (
                abort_after_source_errors > 0
                and consecutive_source_error_tickers >= abort_after_source_errors
            ):
                LOGGER.error(
                    "Dừng financial_ratio sớm sau %s ticker liên tiếp lỗi nguồn (upstream có thể đang outage).",
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
            ratio_df, cfg.data_lake_root, "financial_ratio", cfg.run_date, symbol
        )
        outputs[symbol] = str(out_file)
        LOGGER.info(
            "✓ ratio %s: %s dòng (src=%s, period=%s)",
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
        LOGGER.warning("price_board_snapshot rỗng mọi nguồn — bỏ qua lưu.")
        return ""
    if not isinstance(raw, pd.DataFrame):
        raw = pd.DataFrame(raw)
    df = raw.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["_".join([str(x) for x in c if str(x) != ""]).strip("_") for c in df.columns]
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    df["snapshot_date"] = cfg.run_date
    df["data_source"] = src_used
    out_file = save_partition_parquet(
        df, cfg.data_lake_root, "price_board", cfg.run_date, "price_board_snapshot"
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
