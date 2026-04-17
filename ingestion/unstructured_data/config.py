from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Iterable

import pandas as pd


@dataclass
class NewsIngestionConfig:
    tickers: list[str] = field(default_factory=list)
    use_listing_tickers: bool = False
    listing_parquet_path: Path | None = None
    listing_exchange_filter: list[str] | None = None
    max_tickers_per_run: int = 50

    rss_feed_urls: list[str] = field(default_factory=list)
    sources_yaml_path: Path | None = None

    enable_vnstock: bool = True
    enable_rss: bool = True
    enable_html: bool = True

    days_back: int = 1
    days_back_vnstock: int | None = 500
    days_back_rss: int | None = 1
    days_back_html: int | None = 1
    rate_limit_rpm: int = 20
    max_articles_per_source: int = 200

    append_only: bool = True
    truncate_partition: bool = True

    enable_ticker_match: bool = True
    http_user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    )

    api_retry_max_attempts: int = 4
    api_retry_base_delay_sec: float = 1.5
    timeout_sec: int = 30

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

    @property
    def http_headers(self) -> dict[str, str]:
        return {
            "User-Agent": self.http_user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "vi,en-US;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
        }

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
