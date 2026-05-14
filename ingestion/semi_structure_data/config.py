from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path


@dataclass
class SemiStructuredIngestionConfig:
    data_lake_root: Path = field(
        default_factory=lambda: Path(__file__).resolve().parents[2] / "data-lake" / "raw"
    )
    sources: list[str] = field(default_factory=lambda: ["hnx"])
    tickers: list[str] = field(default_factory=list)
    incremental_window_days: int = 30
    rate_limit_rpm: int = 10
    api_retry_max_attempts: int = 4
    api_retry_base_delay_sec: float = 1.5
    # Retry rieng cho download PDF de tranh treo lau khi host FTP on dinh kem.
    download_retry_max_attempts: int = 3
    min_pdf_bytes: int = 20_000
    bootstrap_full_history_if_missing: bool = True
    full_bootstrap_once_then_incremental: bool = False
    full_bootstrap_marker_file: str = "_full_bootstrap_done.json"
    run_partition: str | None = None
    hnx_sample_json: Path | None = None
    # CSV hang loat: url_pdf,ticker,title,... — xem README semi_structure_data
    hnx_urls_csv: Path | None = None
    request_timeout_sec: int = 45
    # HNX: False = khong verify SSL (thuong can tren Windows/dev de crawl duoc). True = dung certifi.
    hnx_verify_ssl: bool = False
    # True = chi giu tin thoa bo loc tieu de BCTC nam; False = lay het PDF tu nguon (de xem du lieu).
    strict_bctc_annual_keyword_filter: bool = False
    # True = chi tai PDF BCTC (hop nhat / rieng) ban tieng Viet; khong tai EN, giai trinh, ...
    # Tat: BCTC_INGEST_ALL_CRAWLED_PDFS=1 hoac ingest_only_financial_statement_vi=False.
    ingest_only_financial_statement_vi: bool = True
    # UNKNOWN language + BCTC: van tai (mac dinh). Tat neu muon chi VI ro rang.
    ingest_unknown_language_financial: bool = True
    # True = cho phep tai lieu EN tham gia parse/canonical/download (khi filter bat). Hoac env BCTC_ALLOW_EN_DOCS=1.
    allow_en_docs_for_parse: bool = False
    # API cong bo HNX (neu co): uu tien field nay, khong thi bien moi truong HNX_DISCLOSURE_API_URL
    hnx_disclosure_api_url: str | None = None

    def resolved_hnx_disclosure_api_url(self) -> str | None:
        raw = (self.hnx_disclosure_api_url or os.environ.get("HNX_DISCLOSURE_API_URL") or "").strip()
        return raw or None

    def resolved_allow_en_docs_for_parse(self) -> bool:
        env = os.environ.get("BCTC_ALLOW_EN_DOCS", "").strip().lower()
        if env in ("1", "true", "yes"):
            return True
        if env in ("0", "false", "no"):
            return False
        return bool(self.allow_en_docs_for_parse)

    def resolved_ingest_only_financial_statement_vi(self) -> bool:
        """Mac dinh True: chi ingest tai lieu BCTC ban VI. Tat = tai moi URL crawl."""
        env = os.environ.get("BCTC_INGEST_ALL_CRAWLED_PDFS", "").strip().lower()
        if env in ("1", "true", "yes"):
            return False
        return bool(self.ingest_only_financial_statement_vi)

    @property
    def run_date(self) -> str:
        if self.run_partition:
            return str(self.run_partition).strip()
        return date.today().isoformat()

    @property
    def semi_structure_root(self) -> Path:
        return self.data_lake_root / "Semi_Structure_Data"

    @property
    def bctc_pdf_root(self) -> Path:
        return self.semi_structure_root / "bctc_annual_pdf"

    @property
    def bctc_pdf_meta_root(self) -> Path:
        return self.semi_structure_root / "bctc_annual_pdf_meta"

    def marker_path_for_source(self, source: str) -> Path:
        source_key = (source or "unknown").strip().lower()
        return self.bctc_pdf_root / f"source={source_key}" / self.full_bootstrap_marker_file
