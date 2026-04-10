from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timezone

import pandas as pd
from vnstock import Company, Finance, Listing, Trading

from .common import call_with_retry, save_partition_parquet, wait_for_rate_limit
from .config import IngestionConfig

LOGGER = logging.getLogger(__name__)


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


def ingest_listing(cfg: IngestionConfig | None = None) -> str:
    cfg = cfg or IngestionConfig()
    df = pd.DataFrame()
    for src in cfg.resolved_data_sources():
        try:

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
                LOGGER.info("all_symbols: %s mã từ %s", len(df), src.upper())
                break
        except Exception as ex:
            LOGGER.warning("all_symbols(%s) lỗi: %s", src, ex)
    if df.empty:
        LOGGER.warning("Không lấy được listing")
        return ""
    df.columns = [str(c).strip().lower() for c in df.columns]
    if "symbol" in df.columns:
        df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    exchange_rows: list[dict] = []
    index_exchange_map = {
        "VN30": "HOSE",
        "VNMIDCAP": "HOSE",
        "VNSML": "HOSE",
        "VN100": "HOSE",
        "VNALL": "HOSE",
        "HNX30": "HNX",
        "HNXINDEX": "HNX",
    }
    try:
        listing_obj = Listing(source=cfg.resolved_data_sources()[0])
        for idx_code, exchange in index_exchange_map.items():
            try:
                if not hasattr(listing_obj, "symbols_by_group"):
                    continue
                idx_df = listing_obj.symbols_by_group(group=idx_code)
                if idx_df is None or idx_df.empty:
                    continue
                idx_df.columns = [str(c).strip().lower() for c in idx_df.columns]
                sym_col = next((c for c in ["symbol", "ticker", "code"] if c in idx_df.columns), None)
                if not sym_col:
                    continue
                for sym in idx_df[sym_col].dropna().astype(str).str.strip().str.upper().unique():
                    exchange_rows.append({"symbol": sym, "exchange": exchange})
            except Exception as ex:
                LOGGER.debug("symbols_by_group(%s) lỗi: %s", idx_code, ex)
    except Exception as ex:
        LOGGER.warning("build exchange map lỗi: %s", ex)
    if exchange_rows:
        emap = pd.DataFrame(exchange_rows)
        prio = {"HOSE": 0, "HNX": 1, "UPCOM": 2}
        emap["_p"] = emap["exchange"].map(prio).fillna(9)
        emap = emap.sort_values("_p").drop_duplicates("symbol", keep="first").drop(columns=["_p"])
        df = df.drop(columns=["exchange"], errors="ignore").merge(emap, on="symbol", how="left")
    else:
        df["exchange"] = pd.NA
    df["exchange"] = df["exchange"].fillna("UNKNOWN")
    before = len(df)
    if "type" in df.columns:
        df = df[df["type"] == "stock"].copy()
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
    rows: list[dict] = []
    n_run = min(len(cfg.tickers), cfg.max_tickers_per_run)
    for idx, symbol in enumerate(cfg.tickers[: cfg.max_tickers_per_run]):
        LOGGER.info("[%s/%s] Đang tải company %s...", idx + 1, n_run, symbol)
        row: dict = {}
        src_used = ""
        for src in cfg.resolved_data_sources():
            try:

                def _overview() -> object:
                    wait_for_rate_limit(cfg.rate_limit_rpm)
                    company = Company(source=src, symbol=symbol)
                    return (
                        company.overview()
                        if hasattr(company, "overview")
                        else company.profile()
                    )

                raw = call_with_retry(
                    _overview,
                    max_attempts=cfg.api_retry_max_attempts,
                    base_delay_sec=cfg.api_retry_base_delay_sec,
                    label=f"{symbol}@{src}",
                )
                if isinstance(raw, pd.DataFrame):
                    row = raw.iloc[0].to_dict() if not raw.empty else {}
                elif isinstance(raw, dict):
                    row = raw.copy()
                else:
                    row = {}
                if row:
                    src_used = src
                    break
            except Exception as ex:
                LOGGER.warning("Company %s (%s): %s", symbol, src, ex)
        if not src_used:
            LOGGER.warning("Company %s: không lấy được từ nguồn nào", symbol)
            if idx < n_run - 1:
                time.sleep(cfg.inter_request_delay_sec)
            continue
        row["ticker"] = symbol
        row["source"] = src_used
        rows.append(row)
        if idx < min(len(cfg.tickers), cfg.max_tickers_per_run) - 1:
            time.sleep(cfg.inter_request_delay_sec)
    df_new = pd.DataFrame(rows)
    if df_new.empty:
        LOGGER.warning("company_df rỗng — bỏ qua lưu.")
        return ""
    df_new = _clean_text_columns(df_new)
    df_new["ticker"] = df_new["ticker"].astype(str).str.strip().str.upper()
    df_new = df_new.drop_duplicates(subset=["ticker"], keep="last")
    if "source" not in df_new.columns:
        df_new["source"] = cfg.primary_source
    df_new["snapshot_date"] = cfg.run_date
    df_new["fetched_at"] = datetime.now(timezone.utc).isoformat()
    df_new["ingest_run_id"] = str(uuid.uuid4())
    out_file = cfg.data_lake_root / "company" / "master" / "company_overview.parquet"
    out_file.parent.mkdir(parents=True, exist_ok=True)
    if out_file.exists():
        try:
            df_old = pd.read_parquet(out_file)
        except Exception:
            LOGGER.exception("Không đọc được file cũ %s, sẽ tạo mới.", out_file)
            df_old = pd.DataFrame()
    else:
        df_old = pd.DataFrame()
    if df_old.empty:
        combined = df_new
    else:
        cols = list(dict.fromkeys(list(df_old.columns) + list(df_new.columns)))
        combined = pd.concat(
            [df_old.reindex(columns=cols), df_new.reindex(columns=cols)],
            ignore_index=True,
            sort=False,
        )
    try:
        combined.to_parquet(out_file, engine="pyarrow", index=False)
    except ImportError:
        combined.to_parquet(out_file, engine="fastparquet", index=False)
    LOGGER.info("✓ company master append: +%s dòng, tổng %s dòng -> %s", len(df_new), len(combined), out_file)
    return str(out_file)


def _fetch_finance_ratio_one(
    symbol: str, cfg: IngestionConfig
) -> tuple[pd.DataFrame, str, str]:
    for src in cfg.resolved_data_sources():
        for period in ("quarter", "year"):
            try:

                def _ratio() -> pd.DataFrame:
                    wait_for_rate_limit(cfg.rate_limit_rpm)
                    fin = Finance(source=src, symbol=symbol)
                    return fin.ratio(period=period)

                ratio_df = call_with_retry(
                    _ratio,
                    max_attempts=cfg.api_retry_max_attempts,
                    base_delay_sec=cfg.api_retry_base_delay_sec,
                    label=f"ratio {symbol}@{src}/{period}",
                )
            except Exception as ex:
                LOGGER.warning("Finance ratio %s (%s, %s): %s", symbol, src, period, ex)
                continue
            if ratio_df is not None and not ratio_df.empty:
                return ratio_df.copy(), src, period
    return pd.DataFrame(), "", ""


def ingest_financial_ratio(cfg: IngestionConfig | None = None) -> dict[str, str]:
    cfg = cfg or IngestionConfig()
    outputs: dict[str, str] = {}
    n_run = min(len(cfg.tickers), cfg.max_tickers_per_run)
    for idx, symbol in enumerate(cfg.tickers[: cfg.max_tickers_per_run]):
        LOGGER.info("[%s/%s] Đang tải financial ratio %s...", idx + 1, n_run, symbol)
        ratio_df, src_used, period_used = _fetch_finance_ratio_one(symbol, cfg)
        if ratio_df.empty:
            LOGGER.warning("Bỏ qua %s — financial_ratio rỗng mọi nguồn/kỳ", symbol)
            if idx < n_run - 1:
                time.sleep(cfg.inter_request_delay_sec)
            continue
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
