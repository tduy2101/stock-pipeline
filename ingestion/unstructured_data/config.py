from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path


@dataclass
class NewsIngestionConfig:
    """Paths under ``data-lake/raw/Unstructure_Data`` for news ingestion."""

    tickers: list[str] = field(default_factory=list)
    rss_feed_urls: list[str] = field(default_factory=list)
    sources_yaml_path: Path | None = None
    #: Nếu True: lấy danh sách mã từ ``listing.parquet`` (cùng layout structure pipeline),
    #: giới hạn bởi ``max_tickers_per_run``. vnstock không có API “tin toàn sàn”; phải gọi theo từng mã.
    use_listing_tickers: bool = False
    listing_parquet_path: Path | None = None
    #: Chỉ giữ bài có ``published_at`` trong số ngày gần nhất (vd. 7 = một tuần). 0 = không lọc.
    days_back: int = 7
    #: Khi lọc theo ngày, có giữ bài không có ngày đăng hay không.
    keep_undated_when_filtering: bool = False
    #: Khi ``use_listing_tickers=True``: chỉ giữ mã thuộc các sàn này (vd. ``["HOSE","HNX"]``).
    #: None = không lọc theo sàn. Khuyến nghị đề tài: giới hạn sàn để tập mã có ý nghĩa.
    listing_exchange_filter: list[str] | None = None
    #: Sắp xếp mã trước khi cắt ``max_tickers_per_run`` (ổn định, dễ mô tả trong luận văn).
    listing_symbols_sort: bool = True
    primary_vnstock_source: str = "kbs"
    fallback_vnstock_source: str = "vci"
    rate_limit_rpm: int = 30
    max_articles_per_source: int = 200
    inter_request_delay_sec: float = 0.5
    enable_vnstock: bool = True
    enable_rss: bool = True
    enable_html: bool = False
    max_tickers_per_run: int = 50
    api_retry_max_attempts: int = 4
    api_retry_base_delay_sec: float = 1.5
    vnstock_api_key_env: str = "VNSTOCK_API_KEY"
    save_raw_html: bool = False
    #: True: ghi 3 file ``news/vnstock/``, ``news/rss/``, ``news/html/`` (partition ``date=``).
    #: False: gộp một file ``news/items/`` như trước.
    split_news_output_by_source: bool = True

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
    def news_items_dir(self) -> Path:
        """Partitioned Parquet under ``news/items/date=<run_date>/``."""
        return self.news_root / "items"

    @property
    def raw_html_root(self) -> Path:
        """Optional on-disk HTML snapshots when ``save_raw_html`` is True."""
        return self.news_root / "raw_html"

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

    def resolved_vnstock_sources(self) -> list[str]:
        out: list[str] = []
        for s in (self.primary_vnstock_source, self.fallback_vnstock_source):
            key = (s or "").strip().lower()
            if key and key not in out:
                out.append(key)
        return out if out else ["kbs"]
