"""
pipeline_all.py  —  Duy nhất 1 file .py chạy toàn bộ 3 luồng lấy dữ liệu raw.

    1) Có cấu trúc   (Structure_Data)      — giá, index, listing, company, …
    2) Phi cấu trúc   (Unstructure_Data)    — tin tức RSS / HTML
    3) Bán cấu trúc   (Semi_Structure_Data) — BCTC PDF qua discovery VCI

Chạy từ root repo:
    python -m ingestion.pipeline_all              # full (mặc định)
    python -m ingestion.pipeline_all --no-structure --no-bctc   # chỉ tin tức
"""
from __future__ import annotations

import argparse
import logging
import time
from typing import Any

from ingestion.common import configure_logging, load_dotenv_from_project_root

from ingestion.semi_structure_data import BctcPdfConfig, ingest_bctc_pdfs
from ingestion.structure_data.config import IngestionConfig
from ingestion.structure_data.pipeline import (
    run_financial_ratio_ingestion_pipeline,
    run_structure_ingestion_pipeline,
)
from ingestion.unstructured_data import (
    NewsIngestionConfig,
    ingest_news,
)

LOGGER = logging.getLogger(__name__)


def run_full_raw_pipeline(
    structure_cfg: IngestionConfig | None = None,
    news_cfg: NewsIngestionConfig | None = None,
    bctc_cfg: BctcPdfConfig | None = None,
    *,
    include_structure: bool = True,
    include_financial_ratio: bool = False,
    include_news: bool = True,
    include_bctc: bool = True,
    bctc_refresh_listing: bool = True,
    delay_between_groups_sec: int = 30,
) -> dict[str, Any]:
    """
    Run structure → news → BCTC.  Trả dict tổng hợp kết quả mỗi nhóm.
    """
    out: dict[str, Any] = {}
    structure_cfg = structure_cfg or IngestionConfig()
    news_cfg = news_cfg or NewsIngestionConfig()
    bctc_cfg = bctc_cfg or BctcPdfConfig()

    # ── 1. Có cấu trúc (daily: không gồm financial_ratio) ──────────
    if include_structure:
        LOGGER.info("pipeline: [1/3] structure ingestion")
        out["structure"] = run_structure_ingestion_pipeline(
            structure_cfg,
            include_financial_ratio=False,
        )

    # ── 1b. Financial ratio chạy tách lịch (weekly/monthly) ───────
    if include_financial_ratio:
        LOGGER.info("pipeline: [1b/3] financial_ratio ingestion (separate schedule)")
        out["financial_ratio"] = run_financial_ratio_ingestion_pipeline(structure_cfg)[
            "financial_ratio"
        ]

    ran_structure_group = include_structure or include_financial_ratio
    if delay_between_groups_sec > 0 and ran_structure_group and (include_news or include_bctc):
        LOGGER.info("pipeline: pause %ss between groups", delay_between_groups_sec)
        time.sleep(delay_between_groups_sec)

    # ── 2. Phi cấu trúc (tin tức) ───────────────────────────────────
    if include_news:
        LOGGER.info("pipeline: [2/3] news ingestion")
        if not news_cfg.tickers:
            news_cfg.tickers = list(structure_cfg.tickers)
        news_paths = ingest_news(news_cfg)
        out["news_paths"] = news_paths
        out["news_path"] = (
            news_paths.get("rss", {}).get("parquet", "")
            or news_paths.get("html", {}).get("parquet", "")
        )

    if delay_between_groups_sec > 0 and include_news and include_bctc:
        time.sleep(delay_between_groups_sec)

    # ── 3. Bán cấu trúc (BCTC PDF) ─────────────────────────────────
    if include_bctc:
        LOGGER.info("pipeline: [3/3] BCTC PDF ingestion")
        out["bctc"] = ingest_bctc_pdfs(
            bctc_cfg,
            refresh_listing=bctc_refresh_listing,
            structure_cfg=structure_cfg,
            vnstock_api_key_env="VNSTOCK_API_KEY",
        )

    return out


# ── CLI entrypoint ──────────────────────────────────────────────────
def _cli() -> None:
    parser = argparse.ArgumentParser(
        description="Run raw ingestion pipeline (structure → news → BCTC).",
    )
    parser.add_argument("--no-structure", action="store_true", help="Skip structured data")
    parser.add_argument(
        "--include-financial-ratio",
        action="store_true",
        help="Run financial_ratio in this execution (default off for daily DAG)",
    )
    parser.add_argument("--no-news", action="store_true", help="Skip news")
    parser.add_argument("--no-bctc", action="store_true", help="Skip BCTC PDF")
    parser.add_argument("--delay", type=int, default=30, help="Pause (sec) between groups")
    parser.add_argument("--max-tickers", type=int, default=50, help="Max tickers for news")
    parser.add_argument("--max-symbols-bctc", type=int, default=None, help="Max symbols for BCTC (None=all)")
    args = parser.parse_args()

    configure_logging()
    load_dotenv_from_project_root()

    structure_cfg = IngestionConfig()
    news_cfg = NewsIngestionConfig(
        use_listing_tickers=True,
        listing_exchange_filter=["HSX", "HNX"],
        max_tickers_per_run=args.max_tickers,
        days_back=0,
        enable_rss=True,
        enable_html=True,
    )
    bctc_cfg = BctcPdfConfig(
        exchanges=["HSX", "HNX", "UPCOM"],
        discovery_max_news_pages=15,
        max_symbols_per_run=args.max_symbols_bctc,
    )

    out = run_full_raw_pipeline(
        structure_cfg,
        news_cfg,
        bctc_cfg,
        include_structure=not args.no_structure,
        include_financial_ratio=args.include_financial_ratio,
        include_news=not args.no_news,
        include_bctc=not args.no_bctc,
        delay_between_groups_sec=args.delay,
    )

    LOGGER.info("pipeline done: %s", {k: v for k, v in out.items() if k != "structure"})


if __name__ == "__main__":
    _cli()
