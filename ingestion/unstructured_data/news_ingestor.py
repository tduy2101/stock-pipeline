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

LOGGER = logging.getLogger(__name__)


def _load_sources_yaml(path: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
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
    rss_specs: list[dict[str, Any]] = []
    for item in rss:
        if isinstance(item, str):
            url = item.strip()
            if url:
                rss_specs.append({"url": url})
            continue
        if isinstance(item, dict):
            if item.get("enabled", True) is False:
                continue
            url = item.get("url") or item.get("feed_url") or item.get("link")
            url = str(url).strip() if url else ""
            if not url:
                continue
            label = item.get("label") or item.get("id") or item.get("source")
            rss_specs.append({"url": url, "label": label})
    html_specs = [x for x in html if isinstance(x, dict)]
    return rss_specs, html_specs


def _merge_rss_specs(
    raw_urls: list[str], yaml_specs: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[str] = set()
    for url in raw_urls:
        text = str(url).strip()
        if not text or text in seen:
            continue
        merged.append({"url": text})
        seen.add(text)
    for spec in yaml_specs:
        url = str(spec.get("url", "")).strip()
        if not url:
            continue
        if url in seen:
            for item in merged:
                if item.get("url") == url and spec.get("label"):
                    item["label"] = spec.get("label")
                    break
            continue
        merged.append({"url": url, "label": spec.get("label")})
        seen.add(url)
    return merged


def _filter_days_back(
    df: pd.DataFrame,
    days_back: int,
    *,
    strict: bool = False,
) -> pd.DataFrame:
    if df.empty or days_back <= 0:
        return df
    if "published_at" not in df.columns:
        return empty_news_frame() if strict else df
    dt = pd.to_datetime(df["published_at"], errors="coerce", utc=True)
    cutoff = pd.Timestamp.utcnow() - pd.Timedelta(days=days_back)
    if strict:
        keep = dt >= cutoff
    else:
        keep = dt.isna() | (dt >= cutoff)
    return df.loc[keep].copy()


def _resolve_days_back(cfg: NewsIngestionConfig, category: str) -> int:
    per_source = {
        "vnstock": cfg.days_back_vnstock,
        "rss": cfg.days_back_rss,
        "html": cfg.days_back_html,
    }.get(category)
    if per_source is None:
        return cfg.days_back
    return int(per_source)


def save_news(
    df: pd.DataFrame,
    root: Path,
    category: str,
    run_date: str,
    *,
    append_only: bool = True,
    truncate_partition: bool = True,
) -> dict[str, str]:
    out_dir = root / category / f"date={run_date}"
    out_dir.mkdir(parents=True, exist_ok=True)
    if truncate_partition:
        for old in out_dir.glob("PART-*.parquet"):
            try:
                old.unlink()
            except FileNotFoundError:
                pass
        for old in out_dir.glob("PART-*.csv"):
            try:
                old.unlink()
            except FileNotFoundError:
                pass
    parquet_path = out_dir / "PART-000.parquet"
    csv_path = out_dir / "PART-000.csv"

    if append_only:
        LOGGER.debug(
            "save_news[%s]: fixed PART-000 layout is enforced; append_only flag is ignored",
            category,
        )

    out = df.copy()
    for col in ("published_at", "fetched_at"):
        if col in out.columns:
            out[col] = pd.to_datetime(out[col], errors="coerce", utc=True)
    out.to_parquet(parquet_path, index=False)

    csv_df = out.copy()
    for col in ("published_at", "fetched_at"):
        if col in csv_df.columns:
            csv_df[col] = csv_df[col].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            csv_df[col] = csv_df[col].fillna("")
    csv_df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    return {"parquet": str(parquet_path), "csv": str(csv_path)}


def ingest_news(cfg: NewsIngestionConfig | None = None) -> dict[str, dict[str, str] | dict[str, int]]:
    cfg = cfg or NewsIngestionConfig()
    yaml_rss, html_specs = _load_sources_yaml(cfg.resolved_sources_yaml())
    rss_specs = _merge_rss_specs(cfg.rss_feed_urls, yaml_rss)

    vnstock_df = empty_news_frame()
    rss_df = empty_news_frame()
    html_df = empty_news_frame()

    if cfg.prefer_rss_html:
        if cfg.enable_vnstock:
            LOGGER.info("ingest_news: prefer_rss_html=True (vnstock treated as best-effort source)")
        else:
            LOGGER.info("ingest_news: prefer_rss_html=True (RSS+HTML focus mode)")

    if cfg.enable_vnstock:
        try:
            from .vnstock_news_adapter import fetch_vnstock_news

            vnstock_df = fetch_vnstock_news(cfg)
        except Exception as ex:
            LOGGER.warning(
                "ingest_news[vnstock] best-effort fallback: ex_type=%s ex=%s",
                type(ex).__name__,
                ex,
            )
            vnstock_df = empty_news_frame()
    if cfg.enable_rss:
        rss_df = fetch_rss_news(cfg, rss_specs)
    if cfg.enable_html:
        html_df = fetch_html_list_news(cfg, html_specs)

    per_source: dict[str, pd.DataFrame] = {}
    if cfg.enable_vnstock:
        per_source["vnstock"] = vnstock_df
    if cfg.enable_rss:
        per_source["rss"] = rss_df
    if cfg.enable_html:
        per_source["html"] = html_df

    if not per_source:
        LOGGER.warning("ingest_news: no source enabled")
        return {"row_counts": {}}

    output: dict[str, dict[str, str] | dict[str, int]] = {"row_counts": {}}

    for category, df in per_source.items():
        days_back = _resolve_days_back(cfg, category)
        before_filter_count = len(df)
        filtered = _filter_days_back(
            df,
            days_back,
            strict=cfg.strict_published_at_days_back,
        )
        after_filter_count = len(filtered)
        cur = dedupe_news(filtered)
        after_dedupe_count = len(cur)
        LOGGER.info(
            "ingest_news[%s] counts: before_filter=%d after_filter=%d after_dedupe=%d days_back=%d strict=%s",
            category,
            before_filter_count,
            after_filter_count,
            after_dedupe_count,
            days_back,
            cfg.strict_published_at_days_back,
        )
        issues = validate_news_df(cur)
        if issues:
            raise ValueError(f"Validation failed for {category}: {issues}")
        saved = save_news(
            cur[NEWS_COLUMNS],
            cfg.news_root,
            category,
            cfg.run_date,
            append_only=cfg.append_only,
            truncate_partition=cfg.truncate_partition,
        )
        output[category] = saved
        output["row_counts"][category] = after_dedupe_count
        LOGGER.info("ingest_news[%s]: %d rows", category, after_dedupe_count)

    return output
