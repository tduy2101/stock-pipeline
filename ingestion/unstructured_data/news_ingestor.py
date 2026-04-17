"""One-layer news ingestion for unstructured_data."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from .config import NewsIngestionConfig
from .html_list_adapter import fetch_html_list_news
from .rss_adapter import fetch_rss_news
from .schema import NEWS_COLUMNS, dedupe_news, empty_news_frame, validate_news_df
from .vnstock_news_adapter import fetch_vnstock_news

LOGGER = logging.getLogger(__name__)


def _load_sources_yaml(path: Path) -> tuple[list[str], list[dict[str, Any]]]:
    if not path.is_file():
        return [], []
    try:
        import yaml
    except ImportError:
        LOGGER.warning("PyYAML is not installed, sources.yaml is skipped")
        return [], []
    with path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    rss = raw.get("rss_feeds") or []
    html = raw.get("html_sources") or []
    rss_urls = [str(x).strip() for x in rss if str(x).strip()]
    html_specs = [x for x in html if isinstance(x, dict)]
    return rss_urls, html_specs


def _filter_days_back(df: pd.DataFrame, days_back: int) -> pd.DataFrame:
    if df.empty or days_back <= 0:
        return df
    dt = pd.to_datetime(df["published_at"], errors="coerce", utc=True)
    cutoff = pd.Timestamp.utcnow() - pd.Timedelta(days=days_back)
    keep = dt.isna() | (dt >= cutoff)
    return df.loc[keep].copy()


def save_news(df: pd.DataFrame, root: Path, category: str, run_date: str) -> dict[str, str]:
    out_dir = root / category / f"date={run_date}"
    out_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = out_dir / "PART-000.parquet"
    csv_path = out_dir / "PART-000.csv"
    df.to_parquet(parquet_path, index=False)
    df.to_csv(csv_path, index=False, encoding="utf-8")
    return {"parquet": str(parquet_path), "csv": str(csv_path)}


def ingest_news(cfg: NewsIngestionConfig | None = None) -> dict[str, dict[str, str] | dict[str, int]]:
    cfg = cfg or NewsIngestionConfig()
    yaml_rss, html_specs = _load_sources_yaml(cfg.resolved_sources_yaml())
    rss_urls = list(dict.fromkeys([*cfg.rss_feed_urls, *yaml_rss]))

    vnstock_df = empty_news_frame()
    rss_df = empty_news_frame()
    html_df = empty_news_frame()

    if cfg.enable_vnstock:
        vnstock_df = fetch_vnstock_news(cfg)
    if cfg.enable_rss:
        rss_df = fetch_rss_news(cfg, rss_urls)
    if cfg.enable_html:
        html_df = fetch_html_list_news(cfg, html_specs)

    per_source = {"vnstock": vnstock_df, "rss": rss_df, "html": html_df}
    output: dict[str, dict[str, str] | dict[str, int]] = {"row_counts": {}}

    for category, df in per_source.items():
        cur = dedupe_news(_filter_days_back(df, cfg.days_back))
        issues = validate_news_df(cur)
        if issues:
            raise ValueError(f"Validation failed for {category}: {issues}")
        saved = save_news(cur[NEWS_COLUMNS], cfg.news_root, category, cfg.run_date)
        output[category] = saved
        output["row_counts"][category] = len(cur)
        LOGGER.info("ingest_news[%s]: %d rows", category, len(cur))

    return output
