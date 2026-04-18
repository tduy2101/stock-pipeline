from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Iterable

import pandas as pd


@dataclass
class NewsIngestionConfig:
    # Kept for backward compatibility and ticker enrichment/matching in RSS+HTML mode.
    tickers: list[str] = field(default_factory=list)
    # Kept for backward compatibility; now used to source candidate tickers for infer_ticker.
    use_listing_tickers: bool = False
    listing_parquet_path: Path | None = None
    listing_exchange_filter: list[str] | None = None
    max_tickers_per_run: int = 5000

    rss_feed_urls: list[str] = field(default_factory=list)
    sources_yaml_path: Path | None = None

    enable_rss: bool = True
    enable_html: bool = True

    days_back: int = 1
    days_back_rss: int | None = 1
    days_back_html: int | None = 1
    strict_published_at_days_back: bool = False
    rate_limit_rpm: int = 20
    max_articles_per_source: int = 200
    rss_max_per_feed: int = 200
    html_max_per_source: int = 200

    append_only: bool = True
    truncate_partition: bool = True

    enable_ticker_match: bool = True
    http_user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    )
    http_headers: dict[str, str] = field(default_factory=dict)

    api_retry_max_attempts: int = 4
    api_retry_base_delay_sec: float = 1.5
    timeout_sec: int = 30

    def __post_init__(self) -> None:
        base_headers = {
            "User-Agent": self.http_user_agent,
            "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
        }
        extra_headers = self.http_headers if isinstance(self.http_headers, dict) else {}
        merged = {
            **base_headers,
            **{str(k): str(v) for k, v in extra_headers.items() if v is not None},
        }
        self.http_headers = merged

    @property
    def run_date(self) -> str:
        return date.today().isoformat()

    @property
    def data_lake_root(self) -> Path:
        return (
            Path(__file__).resolve().parents[2]
            / "data-lake"
            / "raw"
            / "Unstructure_Data"
        )

    @property
    def news_root(self) -> Path:
        return self.data_lake_root / "news"

    def resolved_sources_yaml(self) -> Path:
        if self.sources_yaml_path is not None:
            return Path(self.sources_yaml_path)
        return Path(__file__).resolve().parent / "sources.yaml"

    def resolved_listing_parquet(self) -> Path:
        if self.listing_parquet_path is not None:
            return Path(self.listing_parquet_path)
        return (
            Path(__file__).resolve().parents[2]
            / "data-lake"
            / "raw"
            / "Structure_Data"
            / "listing"
            / "master"
            / "listing.parquet"
        )

    def resolved_tickers(self) -> list[str]:
        def _clean(values: Iterable[object]) -> list[str]:
            out: list[str] = []
            for val in values:
                text = str(val).strip().upper()
                if not text or text in {"NAN", "NONE", "<NA>"}:
                    continue
                out.append(text)
            return out

        fallback = _clean(self.tickers)
        if not self.use_listing_tickers:
            return fallback[: self.max_tickers_per_run]

        listing_path = self.resolved_listing_parquet()
        if not listing_path.is_file():
            return fallback[: self.max_tickers_per_run]

        try:
            df = pd.read_parquet(listing_path)
        except Exception:
            return fallback[: self.max_tickers_per_run]

        if "symbol" not in df.columns:
            return fallback[: self.max_tickers_per_run]

        if self.listing_exchange_filter and "exchange" in df.columns:
            allow = {s for s in _clean(self.listing_exchange_filter)}
            if allow:
                ex = df["exchange"].astype(str).str.upper().str.strip()
                df = df[ex.isin(allow)]

        symbols = (
            df["symbol"].astype(str).str.upper().str.strip().dropna().unique().tolist()
        )
        cleaned = [s for s in symbols if s and s not in {"NAN", "NONE", "<NA>"}]
        if cleaned:
            return cleaned[: self.max_tickers_per_run]
        return fallback[: self.max_tickers_per_run]
