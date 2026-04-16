"""
Validation / smoke-test utilities for the news pipeline output.

Can be called from notebooks or CLI to verify data quality after ingestion.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from .schemas import ARTICLE_COLUMNS, DISCOVERY_COLUMNS

LOGGER = logging.getLogger(__name__)

_HTML_MARKERS = ("<a", "<img", "<p", "<div", "<span", "<br", "</")


def validate_discovery(df: pd.DataFrame) -> list[str]:
    """Return a list of validation issues (empty = OK)."""
    issues: list[str] = []
    if df.empty:
        issues.append("Discovery DataFrame is empty.")
        return issues

    missing_cols = set(DISCOVERY_COLUMNS) - set(df.columns)
    if missing_cols:
        issues.append(f"Missing columns: {sorted(missing_cols)}")

    n_no_url = (df["canonical_url"].astype(str).str.strip() == "").sum()
    if n_no_url:
        issues.append(f"{n_no_url} rows with empty canonical_url")

    n_dup = df.duplicated(subset=["canonical_url", "source"]).sum()
    if n_dup:
        issues.append(f"{n_dup} duplicate (canonical_url, source) pairs")

    return issues


def validate_articles(df: pd.DataFrame) -> list[str]:
    """Run quality checks on an ARTICLE_COLUMNS DataFrame.

    Returns a list of validation issues (empty = all good).
    """
    issues: list[str] = []
    if df.empty:
        issues.append("Articles DataFrame is empty.")
        return issues

    missing_cols = set(ARTICLE_COLUMNS) - set(df.columns)
    if missing_cols:
        issues.append(f"Missing columns: {sorted(missing_cols)}")

    body = df["body_text"].astype(str)
    summary = df["summary"].astype(str)

    for col_name, series in [("body_text", body), ("summary", summary)]:
        for marker in _HTML_MARKERS:
            n = series.str.contains(marker, na=False).sum()
            if n:
                issues.append(
                    f'{n} rows in "{col_name}" contain HTML marker "{marker}"'
                )

    title = df["title"].astype(str)
    body_eq_title = (body == title) & (body.str.len() > 0)
    n_body_eq_title = body_eq_title.sum()
    if n_body_eq_title:
        issues.append(
            f"{n_body_eq_title} rows where body_text == title (fake body!)"
        )

    n_articles = (df["content_type"] == "article").sum()
    n_with_body = ((df["content_type"] == "article") & (body.str.len() > 0)).sum()
    if n_articles > 0:
        pct = n_with_body / n_articles * 100
        if pct < 50:
            issues.append(
                f"Only {pct:.1f}% of content_type='article' rows have non-empty body_text"
            )

    return issues


def report_articles(df: pd.DataFrame, *, top_n: int = 5) -> str:
    """Generate a human-readable quality report."""
    if df.empty:
        return "No articles to report."

    lines: list[str] = []
    lines.append(f"Total articles: {len(df)}")

    if "content_type" in df.columns:
        ct = df["content_type"].value_counts()
        for k, v in ct.items():
            lines.append(f"  content_type={k}: {v}")

    body_len = df["body_text"].astype(str).str.len()
    lines.append(f"body_text stats: min={body_len.min()}, median={body_len.median():.0f}, max={body_len.max()}")

    n_with_body = (body_len > 0).sum()
    lines.append(f"Rows with non-empty body_text: {n_with_body} ({n_with_body/len(df)*100:.1f}%)")

    lines.append(f"\nTop {top_n} shortest articles (with body):")
    with_body = df[body_len > 0].copy()
    with_body["_blen"] = body_len[body_len > 0]
    for _, row in with_body.nsmallest(top_n, "_blen").iterrows():
        lines.append(
            f"  [{row['_blen']:>5d} chars] {row.get('title', '')[:60]}..."
        )

    lines.append(f"\nTop {top_n} longest articles:")
    for _, row in with_body.nlargest(top_n, "_blen").iterrows():
        lines.append(
            f"  [{row['_blen']:>5d} chars] {row.get('title', '')[:60]}..."
        )

    return "\n".join(lines)
