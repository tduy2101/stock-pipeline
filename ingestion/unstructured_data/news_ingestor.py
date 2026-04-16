from __future__ import annotations

import logging
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from ingestion.common import register_vnstock_api_key_from_env, save_partition_parquet

from .config import NewsIngestionConfig
from .html_list_adapter import HtmlListAdapter
from .news_schema import (
    empty_news_frame,
    filter_news_by_recency,
    finalize_news_frame,
)
from .rss_adapter import RssAdapter
from .sources_loader import load_news_sources_yaml
from .vnstock_news_adapter import VnstockNewsAdapter

LOGGER = logging.getLogger(__name__)

NEWS_OUTPUT_KEYS = ("vnstock", "rss", "html", "combined")


def _load_tickers_from_listing(cfg: NewsIngestionConfig) -> list[str]:
    path = cfg.resolved_listing_parquet()
    if not path.is_file():
        LOGGER.warning(
            "use_listing_tickers=True nhưng không có listing: %s — dùng cfg.tickers.",
            path,
        )
        return list(cfg.tickers)
    df = pd.read_parquet(path)
    if "symbol" not in df.columns:
        LOGGER.warning("listing.parquet không có cột symbol — dùng cfg.tickers.")
        return list(cfg.tickers)
    if cfg.listing_exchange_filter and "exchange" in df.columns:
        allow = {
            str(x).strip().upper()
            for x in cfg.listing_exchange_filter
            if str(x).strip()
        }
        if allow:
            ex = df["exchange"].astype(str).str.strip().str.upper()
            df = df[ex.isin(allow)]
    syms = (
        df["symbol"]
        .astype(str)
        .str.strip()
        .str.upper()
        .dropna()
        .unique()
        .tolist()
    )
    out = [s for s in syms if s and s not in ("NAN", "NONE", "<NA>")]
    if cfg.listing_symbols_sort:
        out = sorted(out)
    cap = max(0, int(cfg.max_tickers_per_run))
    sliced = out[:cap] if cap else out
    if cfg.listing_exchange_filter and sliced:
        LOGGER.info(
            "News listing: sau lọc sàn %s còn %s mã (lấy %s).",
            cfg.listing_exchange_filter,
            len(out),
            len(sliced),
        )
    return sliced


def _filter_frame(
    df: pd.DataFrame, cfg: NewsIngestionConfig, *, label: str
) -> pd.DataFrame:
    if df.empty or cfg.days_back <= 0:
        return df
    n0 = len(df)
    out = filter_news_by_recency(
        df,
        days_back=cfg.days_back,
        keep_undated=cfg.keep_undated_when_filtering,
    )
    LOGGER.info("News [%s]: lọc %s ngày — %s -> %s dòng.", label, cfg.days_back, n0, len(out))
    return out


def _save_one(
    df: pd.DataFrame,
    *,
    cfg: NewsIngestionConfig,
    category: str,
    run_fetched_at: str,
) -> str:
    if df.empty:
        return ""
    final = finalize_news_frame(df, run_fetched_at=run_fetched_at)
    if final.empty:
        return ""
    path = save_partition_parquet(
        final,
        cfg.news_root,
        category,
        cfg.run_date,
        "part-000",
    )
    return str(path)


def ingest_news(cfg: NewsIngestionConfig | None = None) -> dict[str, str]:
    """
    Thu vnstock / RSS / HTML, lọc theo ``days_back``, ghi Parquet.

    Nếu ``split_news_output_by_source=True`` (mặc định): ba file (hoặc ít hơn nếu nguồn tắt/rỗng)::

        news/vnstock/date=<run_date>/PART-000.parquet
        news/rss/date=<run_date>/PART-000.parquet
        news/html/date=<run_date>/PART-000.parquet

    Nếu ``split_news_output_by_source=False``: một file ``news/items/...``.

    Trả về dict các khóa ``vnstock``, ``rss``, ``html``, ``combined`` (đường dẫn rỗng nếu không ghi).
    """
    cfg = cfg or NewsIngestionConfig()
    register_vnstock_api_key_from_env(cfg.vnstock_api_key_env)

    empty_out: dict[str, str] = {k: "" for k in NEWS_OUTPUT_KEYS}

    eff_cfg = cfg
    if cfg.use_listing_tickers:
        loaded = _load_tickers_from_listing(cfg)
        if loaded:
            eff_cfg = replace(cfg, tickers=loaded)
            LOGGER.info(
                "News: dùng %s mã từ listing (tối đa max_tickers_per_run=%s).",
                len(loaded),
                cfg.max_tickers_per_run,
            )

    yaml_rss, html_specs = load_news_sources_yaml(cfg.resolved_sources_yaml())
    rss_urls = list(dict.fromkeys([*cfg.rss_feed_urls, *yaml_rss]))

    run_fetched_at = datetime.now(timezone.utc).isoformat()

    vn_df = VnstockNewsAdapter(eff_cfg).fetch() if cfg.enable_vnstock else empty_news_frame()
    rss_df = (
        RssAdapter(cfg, feed_urls=rss_urls).fetch()
        if cfg.enable_rss
        else empty_news_frame()
    )
    html_df = (
        HtmlListAdapter(cfg, html_specs=html_specs).fetch()
        if cfg.enable_html and html_specs
        else empty_news_frame()
    )

    if vn_df.empty and rss_df.empty and html_df.empty:
        LOGGER.info("ingest_news: không có dữ liệu từ bất kỳ adapter nào — bỏ qua.")
        return empty_out

    vn_df = _filter_frame(vn_df, cfg, label="vnstock")
    rss_df = _filter_frame(rss_df, cfg, label="rss")
    html_df = _filter_frame(html_df, cfg, label="html")

    out: dict[str, str] = dict(empty_out)

    if cfg.split_news_output_by_source:
        out["vnstock"] = _save_one(vn_df, cfg=cfg, category="vnstock", run_fetched_at=run_fetched_at)
        out["rss"] = _save_one(rss_df, cfg=cfg, category="rss", run_fetched_at=run_fetched_at)
        out["html"] = _save_one(html_df, cfg=cfg, category="html", run_fetched_at=run_fetched_at)
        if not any(out[k] for k in ("vnstock", "rss", "html")):
            LOGGER.warning("ingest_news: sau lọc không còn dòng — không ghi file.")
        return out

    merged = pd.concat(
        [vn_df, rss_df, html_df],
        ignore_index=True,
    )
    if merged.empty:
        LOGGER.warning("ingest_news: merged rỗng — skip save.")
        return empty_out
    out["combined"] = _save_one(merged, cfg=cfg, category="items", run_fetched_at=run_fetched_at)
    return out


def primary_news_display_path(paths: dict[str, Any]) -> str:
    """Một đường dẫn gọn để log/UI: ưu tiên combined, sau đó vnstock → rss → html."""
    if not isinstance(paths, dict):
        return str(paths)
    if paths.get("combined"):
        return str(paths["combined"])
    for k in ("vnstock", "rss", "html"):
        if paths.get(k):
            return str(paths[k])
    return ""
