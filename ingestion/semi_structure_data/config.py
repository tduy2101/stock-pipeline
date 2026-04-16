from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path


@dataclass
class BctcPdfConfig:
    """Paths under ``data-lake/raw/Semi_Structure_Data`` for BCTC PDF ingest."""

    quarters_back: int = 8
    #: Mã sàn trong ``listing.parquet`` của vnstock thường là ``HSX`` (HOSE), ``HNX``, ``UPCOM``.
    #: ``HOSE`` được coi là alias của ``HSX`` khi lọc listing.
    exchanges: list[str] = field(
        default_factory=lambda: ["HSX", "HNX", "UPCOM"]
    )
    listing_parquet_path: Path | None = None
    rate_limit_rpm: int = 20
    max_symbols_per_run: int | None = None
    api_retry_max_attempts: int = 4
    api_retry_base_delay_sec: float = 1.5
    user_agent: str = "stock-pipeline-bctc/1.0"
    vnstock_discovery_sources: list[str] = field(
        default_factory=lambda: ["vci"]
    )
    #: Số trang tin VCI/KBS tối đa khi discovery (mỗi trang ~10 bài VCI).
    discovery_max_news_pages: int = 20

    @property
    def run_date(self) -> str:
        return date.today().isoformat()

    @property
    def data_lake_root(self) -> Path:
        return (
            Path(__file__).resolve().parents[2]
            / "data-lake"
            / "raw"
            / "Semi_Structure_Data"
        )

    @property
    def bctc_root(self) -> Path:
        return self.data_lake_root / "bctc"

    @property
    def pdf_root(self) -> Path:
        """``bctc/pdf/exchange=.../symbol=.../year=.../q=...``."""
        return self.bctc_root / "pdf"

    @property
    def manifest_partition_dir(self) -> Path:
        """Per-run manifest partition ``bctc/manifest/date=<run_date>/``."""
        return self.bctc_root / "manifest" / f"date={self.run_date}"

    @property
    def master_manifest_path(self) -> Path:
        """Append-only global manifest ``bctc/master/bctc_files.parquet``."""
        return self.bctc_root / "master" / "bctc_files.parquet"

    def resolved_listing_parquet(self) -> Path:
        """Default: structured listing master next to Structure_Data layout."""
        if self.listing_parquet_path is not None:
            return self.listing_parquet_path
        return (
            Path(__file__).resolve().parents[2]
            / "data-lake"
            / "raw"
            / "Structure_Data"
            / "listing"
            / "master"
            / "listing.parquet"
        )
