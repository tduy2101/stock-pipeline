from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

# VN30 (20) + 30 mã đa ngành — price board mặc định (DAG + notebook demo_50).
DEFAULT_PRICE_BOARD_TICKERS: tuple[str, ...] = (
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
    "VCB",
    "VHM",
    "VIC",
    "VJC",
    "VNM",
    "VPB",
    "VRE",
    "TPB",
    "VIB",
    "HVN",
    "REE",
    "PNJ",
    "GMD",
    "DGC",
    "DPM",
    "DCM",
    "HSG",
    "KDC",
    "QNS",
    "SCS",
    "NVL",
    "PDR",
    "DXG",
    "KDH",
    "HDG",
    "EVF",
    "KBC",
    "AGG",
    "TCH",
    "VPI",
)


def default_price_board_tickers() -> list[str]:
    return list(DEFAULT_PRICE_BOARD_TICKERS)


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
    rate_limit_rpm: int = 50
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
    # --- Listing-as-universe ---
    use_listing_as_universe: bool = True
    listing_exchange_filter: list[str] = field(
        default_factory=lambda: ["HOSE", "HNX"]
    )
    listing_security_type_filter: list[str] = field(default_factory=list)
    # Optional cap after listing load (env STRUCTURED_MAX_TICKERS in Airflow dev/test).
    listing_max_tickers: int | None = None
    # --- Batch ingestion ---
    price_batch_size: int = 100
    price_board_batch_size: int = 50
    delay_between_batches_sec: float = 5.0
    # Price board: mặc định full listing (cfg.tickers sau ingest listing); watchlist = tắt flag này.
    price_board_use_listing_universe: bool = True
    price_board_tickers: list[str] = field(default_factory=default_price_board_tickers)
    price_board_max_tickers: int = 5000
    price_board_batched: bool = True
    # --- Financial ratio universe filter ---
    financial_ratio_exchange_filter: list[str] = field(
        default_factory=lambda: ["HOSE", "HNX"]
    )
    # Daily incremental: start from each ticker's bronze max date (skip if already current).
    use_bronze_ticker_watermark: bool = False

    def __post_init__(self) -> None:
        if not (1 <= self.price_batch_size <= 1000):
            raise ValueError(
                f"price_batch_size must be in [1, 1000], got {self.price_batch_size}"
            )
        if not (1 <= self.price_board_batch_size <= 500):
            raise ValueError(
                "price_board_batch_size must be in [1, 500], "
                f"got {self.price_board_batch_size}"
            )
        if not (1 <= self.price_board_max_tickers <= 5000):
            raise ValueError(
                "price_board_max_tickers must be in [1, 5000], "
                f"got {self.price_board_max_tickers}"
            )
        if self.delay_between_batches_sec < 0:
            raise ValueError(
                "delay_between_batches_sec must be >= 0, "
                f"got {self.delay_between_batches_sec}"
            )

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
