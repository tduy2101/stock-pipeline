from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path


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

    days_back: int = 0
    rate_limit_rpm: int = 30
    max_articles_per_source: int = 200

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
