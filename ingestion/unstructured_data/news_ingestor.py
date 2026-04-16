"""
News ingestion pipeline — three-tier architecture.

1. **discover_news**: collect lightweight discovery records from all adapters.
2. **enrich_news_details**: fetch real article body text from detail pages.
3. **canonicalize_articles**: merge discovery + detail into canonical articles.

The public entry point :func:`ingest_news` orchestrates all three steps and
writes Parquet output files.
"""

from __future__ import annotations

import logging
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from ingestion.common import register_vnstock_api_key_from_env, save_partition_parquet

from .config import NewsIngestionConfig
from .detail import fetch_details
from .html_list_adapter import HtmlListAdapter
from .news_schema import filter_news_by_recency
from .rss_adapter import RssAdapter
from .schemas import (
    ARTICLE_COLUMNS,
    DISCOVERY_COLUMNS,
    compact_text,
    compute_article_id,
    compute_content_hash,
    empty_article_frame,
    empty_discovery_frame,
    looks_like_disclosure,
    strip_html,
)
from .sources_loader import load_news_sources_yaml
from .vnstock_news_adapter import VnstockNewsAdapter

LOGGER = logging.getLogger(__name__)

NEWS_OUTPUT_KEYS = ("vnstock", "rss", "html", "combined", "discovery", "articles")


# ── Ticker loading ──────────────────────────────────────────────────────────

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


# ── Recency filter (operates on discovery schema) ──────────────────────────

def _filter_discovery_by_recency(
    df: pd.DataFrame,
    cfg: NewsIngestionConfig,
    *,
    label: str,
) -> pd.DataFrame:
    """Filter a discovery DataFrame by ``published_at`` recency."""
    if df.empty or cfg.days_back <= 0 or "published_at" not in df.columns:
        return df
    cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=cfg.days_back)
    dt = pd.to_datetime(df["published_at"], errors="coerce", utc=True)
    undated = dt.isna()
    recent = dt >= cutoff
    if cfg.keep_undated_when_filtering:
        keep = recent | undated
    else:
        keep = recent
        if "source_type" in df.columns:
            html_undated = undated & (df["source_type"] == "html_list")
            keep = keep | html_undated
    out = df.loc[keep.fillna(False)].copy()
    LOGGER.info("Discovery [%s]: lọc %d ngày — %d -> %d dòng.", label, cfg.days_back, len(df), len(out))
    return out


# ═══════════════════════════════════════════════════════════════════════════
# STEP 1: DISCOVER
# ═══════════════════════════════════════════════════════════════════════════

def discover_news(cfg: NewsIngestionConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Collect discovery records from all enabled adapters.

    Returns ``(discovery_df, vnstock_articles_df)`` — the second frame is
    non-empty only when VCI provides full article content directly.
    """
    yaml_rss, html_specs = load_news_sources_yaml(cfg.resolved_sources_yaml())
    rss_urls = list(dict.fromkeys([*cfg.rss_feed_urls, *yaml_rss]))

    disc_parts: list[pd.DataFrame] = []
    vnstock_articles = empty_article_frame()

    if cfg.enable_vnstock:
        vn_disc, vn_arts = VnstockNewsAdapter(cfg).fetch()
        disc_parts.append(vn_disc)
        vnstock_articles = vn_arts
        LOGGER.info(
            "discover_news [vnstock]: %d discoveries, %d direct articles",
            len(vn_disc), len(vn_arts),
        )

    if cfg.enable_rss and rss_urls:
        rss_disc = RssAdapter(cfg, feed_urls=rss_urls).fetch()
        disc_parts.append(rss_disc)
        LOGGER.info("discover_news [rss]: %d discoveries", len(rss_disc))

    if cfg.enable_html and html_specs:
        html_disc = HtmlListAdapter(cfg, html_specs=html_specs).fetch()
        disc_parts.append(html_disc)
        LOGGER.info("discover_news [html]: %d discoveries", len(html_disc))

    if not disc_parts:
        return empty_discovery_frame(), vnstock_articles

    all_disc = pd.concat(disc_parts, ignore_index=True)
    all_disc = all_disc.drop_duplicates(
        subset=["canonical_url", "source"], keep="first"
    )
    LOGGER.info("discover_news: total %d unique discoveries", len(all_disc))
    return all_disc, vnstock_articles


# ═══════════════════════════════════════════════════════════════════════════
# STEP 2: ENRICH (detail fetch)
# ═══════════════════════════════════════════════════════════════════════════

def enrich_news_details(
    discovery_df: pd.DataFrame,
    cfg: NewsIngestionConfig,
) -> pd.DataFrame:
    """Fetch article detail pages and return an ARTICLE_COLUMNS DataFrame.

    Only URLs with a registered detail fetcher (VnExpress, CafeF, ...) will
    be fetched.
    """
    if discovery_df.empty or not cfg.enable_detail_fetch:
        return empty_article_frame()
    return fetch_details(discovery_df, cfg)


# ═══════════════════════════════════════════════════════════════════════════
# STEP 3: CANONICALIZE
# ═══════════════════════════════════════════════════════════════════════════

def canonicalize_articles(
    discovery_df: pd.DataFrame,
    detail_articles_df: pd.DataFrame,
    vnstock_articles_df: pd.DataFrame,
    cfg: NewsIngestionConfig,
) -> pd.DataFrame:
    """Merge discovery + detail into a single canonical ARTICLE_COLUMNS frame.

    * Discoveries with detail body_text → ``content_type='article'``
    * Discoveries without detail but with summary → ``content_type='snippet'``
    * Disclosure-looking titles → ``content_type='disclosure'``
    * ``body_text`` is **never** filled from title/summary as fallback.
    """
    fetched_at = datetime.now(timezone.utc).isoformat()

    all_articles_parts: list[pd.DataFrame] = []

    if not detail_articles_df.empty:
        all_articles_parts.append(detail_articles_df)
    if not vnstock_articles_df.empty:
        all_articles_parts.append(vnstock_articles_df)

    detail_urls: set[str] = set()
    if all_articles_parts:
        merged_arts = pd.concat(all_articles_parts, ignore_index=True)
        detail_urls = set(merged_arts["canonical_url"].dropna().unique())
    else:
        merged_arts = empty_article_frame()

    remaining = discovery_df[
        ~discovery_df["canonical_url"].isin(detail_urls)
    ] if not discovery_df.empty else empty_discovery_frame()

    snippet_rows: list[dict[str, Any]] = []
    for _, row in remaining.iterrows():
        title = compact_text(row.get("title", ""))
        summary = compact_text(row.get("summary", ""))
        curl = row.get("canonical_url", "")
        source = row.get("source", "")
        pub = row.get("published_at", "")
        is_disc = looks_like_disclosure(title)

        snippet_rows.append(
            {
                "article_id": compute_article_id(curl, source, pub or ""),
                "discovery_id": row.get("discovery_id", ""),
                "source": source,
                "source_type": row.get("source_type", ""),
                "content_type": "disclosure" if is_disc else "snippet",
                "ticker": "",
                "title": title,
                "summary": summary,
                "body_text": "",
                "url": row.get("url", ""),
                "canonical_url": curl,
                "published_at": pub,
                "fetched_at": fetched_at,
                "language": row.get("language", "vi"),
                "author": "",
                "tags": "",
                "raw_ref": row.get("raw_ref", ""),
                "content_hash": "",
            }
        )

    snippet_df = (
        pd.DataFrame(snippet_rows, columns=ARTICLE_COLUMNS)
        if snippet_rows
        else empty_article_frame()
    )

    final_parts = [p for p in [merged_arts, snippet_df] if not p.empty]
    if not final_parts:
        return empty_article_frame()

    result = pd.concat(final_parts, ignore_index=True)

    result = result.drop_duplicates(subset=["article_id"], keep="first")

    n_total = len(result)
    n_with_body = (result["body_text"].astype(str).str.len() >= cfg.detail_min_body_length).sum()
    LOGGER.info(
        "canonicalize_articles: %d total, %d with body >= %d chars (%.1f%%)",
        n_total,
        n_with_body,
        cfg.detail_min_body_length,
        (n_with_body / n_total * 100) if n_total else 0,
    )

    return result[ARTICLE_COLUMNS]


# ═══════════════════════════════════════════════════════════════════════════
# PUBLIC ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

def ingest_news(cfg: NewsIngestionConfig | None = None) -> dict[str, str]:
    """Run the full news ingestion pipeline.

    Three-tier flow:
    1. **Discovery** — collect links + metadata from all adapters
    2. **Detail fetch** — crawl article pages for real body text
    3. **Canonicalize** — merge into a clean article dataset

    Output Parquet files::

        news/discovery/date=<run_date>/PART-000.parquet
        news/articles/date=<run_date>/PART-000.parquet

    Backward-compatible: also writes per-source files when
    ``split_news_output_by_source=True``.

    Returns a dict with output paths keyed by
    ``discovery``, ``articles``, ``vnstock``, ``rss``, ``html``, ``combined``.
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

    run_fetched_at = datetime.now(timezone.utc).isoformat()

    # ── Step 1: Discover ────────────────────────────────────────────────
    all_disc, vnstock_arts = discover_news(eff_cfg)

    if all_disc.empty:
        LOGGER.info("ingest_news: không có dữ liệu từ bất kỳ adapter nào — bỏ qua.")
        return empty_out

    if cfg.days_back > 0:
        all_disc = _filter_discovery_by_recency(all_disc, cfg, label="all")

    if all_disc.empty:
        LOGGER.warning("ingest_news: sau lọc ngày không còn discovery — bỏ qua.")
        return empty_out

    LOGGER.info("ingest_news: %d discoveries after filtering", len(all_disc))

    out: dict[str, str] = dict(empty_out)

    if cfg.output_discovery:
        path = save_partition_parquet(
            all_disc, cfg.news_root, "discovery", cfg.run_date, "part-000"
        )
        out["discovery"] = str(path)
        LOGGER.info("Wrote discovery: %s (%d rows)", path, len(all_disc))

    # ── Step 2: Enrich detail ───────────────────────────────────────────
    detail_arts = enrich_news_details(all_disc, cfg)
    LOGGER.info(
        "ingest_news: detail fetch returned %d articles", len(detail_arts)
    )

    # ── Step 3: Canonicalize ────────────────────────────────────────────
    articles = canonicalize_articles(all_disc, detail_arts, vnstock_arts, cfg)

    if cfg.output_articles and not articles.empty:
        path = save_partition_parquet(
            articles, cfg.news_root, "articles", cfg.run_date, "part-000"
        )
        out["articles"] = str(path)
        LOGGER.info("Wrote articles: %s (%d rows)", path, len(articles))

    # ── Backward-compat: per-source split (optional) ────────────────────
    if cfg.split_news_output_by_source and not articles.empty:
        for src_key, mask_fn in [
            ("vnstock", lambda df: df["source"].str.startswith("vnstock_")),
            ("rss", lambda df: df["source_type"] == "rss"),
            ("html", lambda df: df["source_type"].isin(["html_list", "html_detail"])),
        ]:
            subset = articles[mask_fn(articles)]
            if not subset.empty:
                p = save_partition_parquet(
                    subset, cfg.news_root, src_key, cfg.run_date, "part-000"
                )
                out[src_key] = str(p)

    return out


def primary_news_display_path(paths: dict[str, Any]) -> str:
    """Return a single path for display; prefers ``articles`` then ``discovery``."""
    if not isinstance(paths, dict):
        return str(paths)
    for k in ("articles", "discovery", "combined", "vnstock", "rss", "html"):
        if paths.get(k):
            return str(paths[k])
    return ""
