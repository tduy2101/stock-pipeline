from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


def default_repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


@dataclass(slots=True)
class SilverConfig:
    """Filesystem configuration for local Bronze-to-Silver transforms."""

    repo_root: Path = field(default_factory=default_repo_root)
    data_lake_dir: str = "data-lake"
    bronze_dir: str = "raw"
    silver_dir: str = "silver"
    structure_dir_name: str = "Structure_Data"
    unstructure_dir_name: str = "Unstructured_Data"
    semi_structure_dir_name: str = "Semi_Structure_Data"
    part_filename: str = "PART-000.parquet"

    @property
    def data_lake_root(self) -> Path:
        return self.repo_root / self.data_lake_dir

    @property
    def bronze_root(self) -> Path:
        return self.data_lake_root / self.bronze_dir

    @property
    def silver_root(self) -> Path:
        return self.data_lake_root / self.silver_dir

    @property
    def structure_bronze_root(self) -> Path:
        return self.bronze_root / self.structure_dir_name

    @property
    def unstructure_bronze_root(self) -> Path:
        return self.bronze_root / self.unstructure_dir_name

    @property
    def semi_structure_bronze_root(self) -> Path:
        return self.bronze_root / self.semi_structure_dir_name

    def price_bronze_dir(self) -> Path:
        return self.structure_bronze_root / "price"

    def index_bronze_dir(self) -> Path:
        return self.structure_bronze_root / "index"

    def listing_bronze_path(self) -> Path:
        return self.structure_bronze_root / "listing" / "master" / "listing.parquet"

    def company_bronze_path(self) -> Path:
        snapshot_root = self.structure_bronze_root / "company" / "snapshots"
        snapshots = sorted(snapshot_root.glob("snapshot_date=*/company_overview.parquet"))
        if snapshots:
            return snapshots[-1]
        return snapshot_root / "snapshot_date=UNKNOWN" / "company_overview.parquet"

    def silver_dataset_dir(self, dataset: str) -> Path:
        return self.silver_root / dataset

    def silver_partition_dir(self, dataset: str, trading_date_partition: str) -> Path:
        return self.silver_dataset_dir(dataset) / f"trading_date={trading_date_partition}"

    def silver_date_partition_dir(self, dataset: str, run_partition: str) -> Path:
        return self.silver_dataset_dir(dataset) / f"date={run_partition}"

    def silver_current_dir(self, dataset: str) -> Path:
        return self.silver_dataset_dir(dataset) / "current"

    def news_bronze_path(self, stream: str, run_partition: str) -> Path:
        return (
            self.unstructure_bronze_root
            / "news"
            / stream
            / f"date={run_partition}"
            / self.part_filename
        )

    def bctc_pdf_meta_bronze_path(
        self,
        run_partition: str,
        *,
        source: str = "hnx",
    ) -> Path:
        return (
            self.semi_structure_bronze_root
            / "bctc_annual_pdf_meta"
            / f"source={source}"
            / f"date={run_partition}"
            / self.part_filename
        )
