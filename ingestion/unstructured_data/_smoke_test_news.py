from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pandas as pd

os.environ.setdefault("PYTHONUTF8", "1")
os.environ.setdefault("PYTHONIOENCODING", "utf-8")
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ingestion.common import (  # noqa: E402
    configure_logging,
    load_dotenv_from_project_root,
)
from ingestion.unstructured_data import (  # noqa: E402
    NewsIngestionConfig,
    ingest_news,
    validate_news_df,
)


def main() -> int:
    configure_logging()
    load_dotenv_from_project_root()

    rate_limit = int(os.getenv("NEWS_RATE_LIMIT_RPM", "60"))

    cfg = NewsIngestionConfig(
        use_listing_tickers=True,
        listing_exchange_filter=["HSX", "HNX", "UPCOM"],
        max_tickers_per_run=500,
        max_articles_per_source=200,
        rss_max_per_feed=200,
        html_max_per_source=200,
        days_back=7,
        days_back_rss=7,
        days_back_html=7,
        strict_published_at_days_back=False,
        rate_limit_rpm=rate_limit,
        enable_rss=True,
        enable_html=True,
        enable_ticker_match=True,
        append_only=False,
        truncate_partition=True,
    )

    print("=== RUN CONFIG ===")
    print(f"run_date: {cfg.run_date}")
    print(f"news_root: {cfg.news_root}")
    print(f"sources_yaml: {cfg.resolved_sources_yaml()}")
    print(f"tickers: {len(cfg.resolved_tickers())}")
    print(
        "focus_mode: RSS+HTML only "
        f"(rss_enabled={cfg.enable_rss} html_enabled={cfg.enable_html})"
    )

    news_paths = ingest_news(cfg)

    print("\n=== ROW COUNTS ===")
    print(json.dumps(news_paths.get("row_counts", {}), ensure_ascii=False, indent=2))

    print("\n=== OUTPUT CHECK ===")
    failed_sources: list[str] = []
    validation_status: dict[str, str] = {}

    for source in ["rss", "html"]:
        info = news_paths.get(source, {})
        parquet_raw = info.get("parquet", "")

        parquet_path = Path(parquet_raw) if parquet_raw else None

        parquet_ok = bool(parquet_path and parquet_path.is_file())

        print(f"{source}: parquet_ok={parquet_ok}")
        if parquet_path:
            print(f"  parquet: {parquet_path}")

        if not parquet_ok:
            failed_sources.append(source)
            validation_status[source] = "failed: parquet output not found"
            continue

        df = pd.read_parquet(parquet_path)
        issues = validate_news_df(df)
        if issues:
            validation_status[source] = f"failed: {issues}"
        else:
            validation_status[source] = "passed"

        print(f"  rows={len(df)}")
        print(f"  validation={validation_status[source]}")
        sample_cols = ["article_id", "source", "ticker", "title", "published_at", "url"]
        sample_cols = [c for c in sample_cols if c in df.columns]
        if sample_cols:
            print("  sample (3 rows):")
            print(df[sample_cols].head(3).to_string(index=False))

    print("\n=== VALIDATION STATUS ===")
    print(json.dumps(validation_status, ensure_ascii=False, indent=2))

    if failed_sources:
        print(f"\nFAILED SOURCES: {failed_sources}")
        return 1

    print("\nAll configured sources (rss, html) produced output files (parquet).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
