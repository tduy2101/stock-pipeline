from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path


@dataclass
class IngestionConfig:
    tickers: list[str] = field(
        default_factory=lambda: [
            "ACB",
            "BCM",
            "BID",
            "BVH",
            "CTG",
            "FPT",
            "GAS",
            "GVR",
            "HDB",
            "HPG",
            "MBB",
            "MSN",
            "MWG",
            "PLX",
            "POW",
            "SAB",
            "SHB",
            "SSI",
            "STB",
            "TCB",
        ]
    )
    index_tickers: list[str] = field(
        default_factory=lambda: ["VNINDEX", "VN30", "HNXINDEX", "HNX30", "UPCOMINDEX"]
    )
    primary_source: str = "kbs"
    fallback_source: str = "vci"
    rate_limit_rpm: int = 10
    years_back: int = 5
    use_incremental_window: bool = True
    incremental_window_days: int = 10
    bootstrap_full_history_if_missing: bool = True
    full_bootstrap_once_then_incremental: bool = False
    full_bootstrap_marker_file: str = "_full_bootstrap_done.json"
    run_partition: str | None = None
    max_tickers_per_run: int = 50
    delay_between_categories_sec: int = 30
    inter_request_delay_sec: float = 0.5
    # QC
    min_ohlcv_rows_stock: int = 50
    min_ohlcv_rows_index: int = 100
    min_ohlcv_rows_stock_incremental: int = 5
    min_ohlcv_rows_index_incremental: int = 5
    api_retry_max_attempts: int = 4
    api_retry_base_delay_sec: float = 1.5
    financial_ratio_retry_max_attempts: int = 2
    financial_ratio_retry_base_delay_sec: float = 1.0
    financial_ratio_disable_source_on_transient_error: bool = True
    financial_ratio_abort_after_consecutive_source_errors: int = 2
    vnstock_api_key_env: str = "VNSTOCK_API_KEY"

    def resolved_data_sources(self) -> list[str]:
        out: list[str] = []
        for s in (self.primary_source, self.fallback_source):
            key = (s or "").strip().lower()
            if key and key not in out:
                out.append(key)
        return out if out else ["kbs"]

    @property
    def run_date(self) -> str:
        if self.run_partition:
            return str(self.run_partition).strip()
        return date.today().isoformat()

    @property
    def start_date(self) -> str:
        today = date.today()
        try:
            start = today.replace(year=today.year - self.years_back)
        except ValueError:
            start = today.replace(year=today.year - self.years_back, day=28)
        return start.isoformat()

    @property
    def end_date(self) -> str:
        return date.today().isoformat()

    @property
    def data_lake_root(self) -> Path:
        return (
            Path(__file__).resolve().parents[2]
            / "data-lake"
            / "raw"
            / "Structure_Data"
        )

    def full_bootstrap_marker_path(self, category: str) -> Path:
        safe_category = str(category).strip().lower()
        return self.data_lake_root / safe_category / self.full_bootstrap_marker_file
