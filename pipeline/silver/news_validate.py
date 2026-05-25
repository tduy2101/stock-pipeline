from __future__ import annotations

import pandas as pd

from .news_transformer import NEWS_OUTPUT_COLUMNS
from .ticker_match import TICKER_BLOCKLIST, find_mentions_in_parts, load_stock_universe
from .config import SilverConfig, default_repo_root

_BLOCKLIST_WARNING_RATIO = 0.30


def validate_news_silver(df: pd.DataFrame) -> list[str]:
    """Return validation messages; lines starting with ERROR: fail strict runs."""
    issues: list[str] = []
    missing = [c for c in NEWS_OUTPUT_COLUMNS if c not in df.columns]
    if missing:
        issues.append(f"ERROR: missing columns: {missing}")
        return issues
    if df.empty:
        return issues

    article_id = df["article_id"].fillna("").astype(str).str.strip()
    if (article_id == "").any():
        issues.append("ERROR: article_id has empty values")
    if article_id.duplicated().any():
        dup = int(article_id.duplicated().sum())
        issues.append(f"ERROR: duplicate article_id rows: {dup}")

    urls = df["url"].fillna("").astype(str).str.strip()
    non_empty_urls = urls[urls != ""]
    if (~non_empty_urls.str.startswith("http")).any():
        issues.append("ERROR: url must start with http when present")
    if non_empty_urls.duplicated().any():
        dup_urls = int(non_empty_urls.duplicated().sum())
        issues.append(f"ERROR: duplicate url values: {dup_urls}")

    titles = df["title"].fillna("").astype(str).str.strip()
    if (titles == "").any():
        issues.append("ERROR: title has empty values")

    has_ticker = df["ticker"].notna() & df["ticker"].astype(str).str.strip().ne("")
    if has_ticker.any():
        blocklist_hits = df.loc[has_ticker, "ticker"].astype(str).str.upper().isin(TICKER_BLOCKLIST)
        ratio = float(blocklist_hits.mean())
        if ratio > _BLOCKLIST_WARNING_RATIO:
            issues.append(
                f"WARN: {ratio:.1%} of ticker values are blocklisted symbols "
                f"({int(blocklist_hits.sum())}/{int(has_ticker.sum())})"
            )

    listing_path = SilverConfig(repo_root=default_repo_root()).listing_bronze_path()
    universe = load_stock_universe(listing_path)
    if universe and has_ticker.any():
        invalid = 0
        for _, row in df.loc[has_ticker].iterrows():
            code = str(row["ticker"]).strip().upper()
            mentions_raw = row.get("ticker_mentions")
            if mentions_raw is None or (isinstance(mentions_raw, float) and pd.isna(mentions_raw)):
                mentions: list[str] = []
            else:
                mentions = list(mentions_raw)
            if code not in universe:
                invalid += 1
            elif code not in mentions:
                invalid += 1
        if invalid:
            issues.append(f"WARN: {invalid} rows have ticker not in ticker_mentions/universe")

    return issues


def blocklist_without_context_count(df: pd.DataFrame, universe: frozenset[str]) -> int:
    """Count rows where blocklisted ticker appears without contextual match in text."""
    if df.empty or not universe:
        return 0
    count = 0
    for _, row in df.iterrows():
        ticker = row.get("ticker")
        if pd.isna(ticker):
            continue
        code = str(ticker).strip().upper()
        if code not in TICKER_BLOCKLIST:
            continue
        mentions = find_mentions_in_parts(
            [row.get("title"), row.get("summary"), row.get("body_text")],
            universe,
        )
        if code not in mentions:
            count += 1
    return count
