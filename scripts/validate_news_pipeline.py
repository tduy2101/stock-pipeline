#!/usr/bin/env python3
"""Read-only validation report for Bronze/Silver news partitions."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.silver.config import SilverConfig
from pipeline.silver.news_validate import validate_news_silver
from pipeline.silver.ticker_match import TICKER_BLOCKLIST


def _read_optional(path: Path) -> pd.DataFrame | None:
    if not path.is_file():
        return None
    return pd.read_parquet(path)


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate news Bronze/Silver partition.")
    parser.add_argument("--run-partition", required=True, help="YYYY-MM-DD")
    parser.add_argument("--repo-root", default=str(ROOT))
    args = parser.parse_args()

    cfg = SilverConfig(repo_root=Path(args.repo_root))
    run_partition = args.run_partition

    rss_path = cfg.news_bronze_path("rss", run_partition)
    html_path = cfg.news_bronze_path("html", run_partition)
    silver_path = cfg.silver_date_partition_dir("news", run_partition) / cfg.part_filename

    print(f"=== News pipeline report: date={run_partition} ===\n")

    errors = 0
    rss = _read_optional(rss_path)
    html = _read_optional(html_path)
    silver = _read_optional(silver_path)

    if rss is None and html is None:
        print("ERROR: no Bronze rss/html partitions found")
        return 1

    if rss is not None:
        print(f"Bronze RSS: {len(rss)} rows -> {rss_path.relative_to(cfg.repo_root)}")
        body = rss["body_text"].fillna("").astype(str).str.strip().ne("").sum()
        print(f"  body_text non-empty: {body}/{len(rss)}")
    if html is not None:
        print(f"Bronze HTML: {len(html)} rows -> {html_path.relative_to(cfg.repo_root)}")
        body = html["body_text"].fillna("").astype(str).str.strip().ne("").sum()
        print(f"  body_text non-empty: {body}/{len(html)}")

    if rss is not None and html is not None:
        overlap = len(set(rss["article_id"]) & set(html["article_id"]))
        union = len(set(rss["article_id"]) | set(html["article_id"]))
        print(f"\nBronze article_id overlap rss/html: {overlap}")
        print(f"Bronze unique article_id (union): {union}")

    if silver is None:
        print("\nERROR: Silver partition missing")
        return 1

    print(f"\nSilver: {len(silver)} rows -> {silver_path.relative_to(cfg.repo_root)}")
    pub = pd.to_datetime(silver["published_at"], errors="coerce", utc=True)
    if pub.notna().any():
        print(f"  published_at range: {pub.min()} .. {pub.max()}")

    has_ticker = silver["ticker"].notna() & silver["ticker"].astype(str).str.strip().ne("")
    print(f"  ticker non-null: {int(has_ticker.sum())}/{len(silver)}")
    if has_ticker.any():
        print("  top tickers:")
        for code, count in silver.loc[has_ticker, "ticker"].value_counts().head(10).items():
            flag = " (blocklist)" if str(code).upper() in TICKER_BLOCKLIST else ""
            print(f"    {code}: {count}{flag}")

    issues = validate_news_silver(silver)
    if issues:
        print("\nValidation messages:")
        for msg in issues:
            print(f"  {msg}")
            if msg.startswith("ERROR:"):
                errors += 1
    else:
        print("\nValidation: OK (no issues)")

    if rss is not None and html is not None and silver is not None:
        bronze_union = len(set(rss["article_id"]) | set(html["article_id"]))
        print(f"\nBronze union vs Silver rows: {bronze_union} -> {len(silver)}")

    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
