"""Reusable task callables for stock-pipeline Airflow DAGs."""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Any, Iterable

LOGGER = logging.getLogger(__name__)

DEFAULT_REPO_ROOT = "/opt/stock-pipeline"
# Daily structured silver datasets (price/index/board only; listing+company are monthly).
STRUCTURED_SILVER_DATASETS = (
    "price",
    "index_price",
    "price_board",
)
# Monthly structured silver datasets (full-universe snapshots).
STRUCTURED_MONTHLY_SILVER_DATASETS = (
    "listing",
    "company",
    "financial_ratio",
)
DBT_PROJECT_DIR = "transform/dbt"
DBT_PROFILES_DIR = "transform/dbt"

# dbt subset selectors aligned with transform/dbt model refs (see Docs/dbt_outputs_and_lineage.md)
# Daily incremental path: fast models + 4 incremental gold tables (no full rebuild).
DBT_STRUCTURED_SELECT = (
    "fact_index_daily mart_price_board "
    "int_price_indicator fact_price_daily mart_stock_daily mart_market_overview"
)
# Legacy full-rebuild selector (backfill / manual use).
DBT_STRUCTURED_SELECT_FULL = (
    "stg_price stg_index_price stg_price_board "
    "int_price_indicator fact_price_daily fact_index_daily "
    "fact_news_article mart_stock_news_signal "
    "mart_stock_daily mart_price_board mart_market_overview"
)
DBT_NEWS_SELECT = "+mart_stock_news_signal +fact_news_article"
DBT_FINANCIAL_RATIO_SELECT = "+mart_financial_summary +mart_company_profile"
# Monthly structured marts: listing dim + company profile + financial summary.
DBT_STRUCTURED_MONTHLY_SELECT = "+mart_financial_summary +mart_company_profile"
DBT_BCTC_SELECT = "+mart_bctc_documents"


def get_repo_root() -> Path:
    """Return mounted stock-pipeline repo root."""
    return Path(os.environ.get("STOCK_PIPELINE_ROOT", DEFAULT_REPO_ROOT)).resolve()


def _base_env(extra: dict[str, str] | None = None) -> dict[str, str]:
    env = os.environ.copy()
    root = str(get_repo_root())
    env["PYTHONPATH"] = root
    env["STOCK_PIPELINE_ROOT"] = root
    if extra:
        env.update(extra)
    return env


def run_subprocess(
    cmd: list[str],
    *,
    cwd: Path | None = None,
    extra_env: dict[str, str] | None = None,
) -> None:
    """Run a CLI command from repo root; raise on non-zero exit."""
    workdir = cwd or get_repo_root()
    env = _base_env(extra_env)
    LOGGER.info("Running: %s (cwd=%s)", " ".join(cmd), workdir)
    proc = subprocess.run(
        cmd,
        cwd=str(workdir),
        env=env,
        check=False,
        text=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"Command failed (exit {proc.returncode}): {' '.join(cmd)}"
        )


def _load_dotenv() -> None:
    try:
        from ingestion.structure_data.common import load_dotenv_from_project_root

        load_dotenv_from_project_root()
    except Exception as ex:
        LOGGER.warning("Could not load .env from project root: %s", ex)


def _require_vnstock_api_key() -> None:
    from ingestion.structure_data import register_vnstock_api_key_from_env

    if not register_vnstock_api_key_from_env():
        raise RuntimeError(
            "VNSTOCK_API_KEY is missing or empty. Set it in repo root .env "
            "(no spaces around =). Do not set empty VNSTOCK_API_KEY= in "
            "docker/airflow/.env.airflow — it overrides the root .env."
        )


def _structured_ingestion_config():
    """
    IngestionConfig for structured_daily DAG.

    Universe (env STRUCTURED_DAG_UNIVERSE):
    - watchlist50 (default): 50 mã cố định DEFAULT_PRICE_BOARD_TICKERS (VN30 + đa ngành)
    - listing: full HOSE/HNX từ listing bronze; optional STRUCTURED_MAX_TICKERS cap
    """
    from ingestion.structure_data import IngestionConfig
    from ingestion.structure_data.config import default_price_board_tickers

    cfg = IngestionConfig()
    universe = os.environ.get("STRUCTURED_DAG_UNIVERSE", "watchlist50").strip().lower()

    if universe in {"watchlist50", "watchlist", "fixed50", "demo50"}:
        tickers = default_price_board_tickers()
        cfg.use_listing_as_universe = False
        cfg.tickers = tickers
        cfg.max_tickers_per_run = len(tickers)
        cfg.price_board_use_listing_universe = False
        LOGGER.info(
            "Structured DAG universe=watchlist50: %s fixed tickers (listing ingest still full snapshot)",
            len(tickers),
        )
        return cfg

    if universe in {"listing", "full", "hose_hnx"}:
        cfg.use_listing_as_universe = True
        cfg.listing_exchange_filter = ["HOSE", "HNX"]
        cfg.listing_security_type_filter = ["stock"]
        cap_raw = os.environ.get("STRUCTURED_MAX_TICKERS", "").strip()
        if cap_raw.isdigit() and int(cap_raw) > 0:
            cfg.listing_max_tickers = int(cap_raw)
            LOGGER.info(
                "Structured DAG universe=listing HOSE/HNX stocks cap=%s (STRUCTURED_MAX_TICKERS)",
                cfg.listing_max_tickers,
            )
        else:
            LOGGER.info("Structured DAG universe=listing HOSE/HNX stocks (full)")
        return cfg

    raise ValueError(
        f"Invalid STRUCTURED_DAG_UNIVERSE={universe!r}. "
        "Use watchlist50 or listing."
    )


def _structured_monthly_config():
    """
    IngestionConfig for structured_monthly DAG (listing + company + financial_ratio).

    Always full HOSE/HNX listing universe regardless of STRUCTURED_DAG_UNIVERSE.
    Optional STRUCTURED_MAX_TICKERS caps the universe (dev/test).
    """
    from ingestion.structure_data import IngestionConfig

    cfg = IngestionConfig()
    cfg.use_listing_as_universe = True
    cfg.listing_exchange_filter = ["HOSE", "HNX"]
    cfg.listing_security_type_filter = ["stock"]
    cap_raw = os.environ.get("STRUCTURED_MAX_TICKERS", "").strip()
    if cap_raw.isdigit() and int(cap_raw) > 0:
        cfg.listing_max_tickers = int(cap_raw)
        LOGGER.info(
            "Structured monthly: HOSE/HNX stocks cap=%s (STRUCTURED_MAX_TICKERS)",
            cfg.listing_max_tickers,
        )
    else:
        LOGGER.info(
            "Structured monthly: full HOSE/HNX stock universe from listing bronze"
        )
    return cfg


def _load_listing_tickers_into_cfg(cfg, *, exchange_filter: list[str] | None = None) -> int:
    """
    Populate cfg.tickers from Bronze listing parquet (exchange + stock filters).

    Required for financial_ratio_weekly: run_financial_ratio_ingestion_pipeline
    intersects exchange tickers with cfg.tickers (default only 20 demo symbols).
    """
    if not cfg.listing_security_type_filter:
        cfg.listing_security_type_filter = ["stock"]
    from ingestion.structure_data.common import load_tickers_from_listing_bronze_in_file_order

    ex = exchange_filter if exchange_filter is not None else cfg.listing_exchange_filter
    tickers = load_tickers_from_listing_bronze_in_file_order(
        cfg,
        exchange_filter=ex,
    )
    cap = getattr(cfg, "listing_max_tickers", None)
    if cap is not None and int(cap) > 0:
        tickers = tickers[: int(cap)]
    if not tickers:
        raise RuntimeError(
            "No tickers from listing bronze. Run structured_daily once or "
            "ingest listing manually before financial_ratio_weekly."
        )
    cfg.tickers = tickers
    cfg.max_tickers_per_run = len(tickers)
    return len(tickers)


def ingest_structured(**_: Any) -> dict[str, Any]:
    """
    Bronze structured daily: price + index + price_board only.

    listing & company are handled monthly (structured_monthly), so daily skips
    them via include_listing=False / include_company=False. Incremental window
    with per-ticker bronze watermark. Optional STRUCTURED_MAX_TICKERS for dev.
    """
    _load_dotenv()
    from ingestion.structure_data import (
        configure_logging,
        run_structure_ingestion_pipeline,
    )

    configure_logging()
    _require_vnstock_api_key()
    cfg = _structured_ingestion_config()
    # Daily DAG sau notebook backfill: incremental only, per-ticker bronze watermark.
    cfg.bootstrap_full_history_if_missing = False
    cfg.use_bronze_ticker_watermark = True
    cfg.delay_between_categories_sec = 5
    cfg.incremental_window_days = 2
    marker = cfg.full_bootstrap_marker_path("price")
    LOGGER.info(
        "Structured bronze (daily): price+index+price_board only "
        "(skip listing+company → monthly); ticker_watermark=%s incremental_days=%s",
        cfg.use_bronze_ticker_watermark,
        cfg.incremental_window_days,
    )
    LOGGER.info(
        "Structured bronze: bootstrap marker=%s exists=%s incremental_window=%s days=%s",
        marker,
        marker.is_file(),
        cfg.use_incremental_window,
        cfg.incremental_window_days,
    )
    result = run_structure_ingestion_pipeline(
        cfg,
        include_listing=False,
        include_company=False,
        include_financial_ratio=False,
    )
    LOGGER.info("Structured bronze finished: keys=%s", list(result.keys()))
    return result


def ingest_structured_monthly(**_: Any) -> dict[str, Any]:
    """
    Bronze structured monthly: listing + company + financial_ratio (full HOSE/HNX).

    Uses the listing-as-universe pipeline branch so company & financial_ratio
    cover every HOSE/HNX ticker loaded from the fresh listing snapshot. Prices,
    indices and price_board are skipped (those run daily).
    """
    _load_dotenv()
    from ingestion.structure_data import (
        configure_logging,
        run_structure_ingestion_pipeline,
    )

    configure_logging()
    _require_vnstock_api_key()
    cfg = _structured_monthly_config()
    cfg.bootstrap_full_history_if_missing = False
    cfg.delay_between_categories_sec = 5
    LOGGER.info(
        "Structured bronze (monthly): listing+company+financial_ratio HOSE/HNX stocks"
    )
    result = run_structure_ingestion_pipeline(
        cfg,
        include_prices=False,
        include_indices=False,
        include_listing=True,
        include_company=True,
        include_financial_ratio=True,
        include_price_board=False,
    )
    LOGGER.info("Structured monthly bronze finished: keys=%s", list(result.keys()))
    return result


def ingest_news(**_: Any) -> str:
    """Bronze news (days_back=1). Returns run_partition YYYY-MM-DD for silver."""
    _load_dotenv()
    from ingestion.unstructured_data import NewsIngestionConfig, ingest_news
    from ingestion.structure_data import configure_logging

    configure_logging()
    cfg = NewsIngestionConfig()
    ingest_news(cfg)
    run_partition = cfg.run_date
    LOGGER.info("News bronze run_partition=%s days_back=%s", run_partition, cfg.days_back)
    return run_partition


def ingest_bctc(**_: Any) -> str:
    """Bronze BCTC quarterly (hnx_max_list_pages=10 default). Returns run_partition."""
    _load_dotenv()
    from ingestion.semi_structure_data import (
        SemiStructuredIngestionConfig,
        run_bctc_annual_pipeline,
    )
    from ingestion.structure_data import configure_logging

    configure_logging()
    cfg = SemiStructuredIngestionConfig()
    cfg.run_partition = date.today().isoformat()
    LOGGER.info(
        "BCTC bronze: run_partition=%s hnx_max_list_pages=%s",
        cfg.run_partition,
        cfg.resolved_hnx_max_list_pages(),
    )
    run_bctc_annual_pipeline(cfg, include_download=True)
    return cfg.run_date


def ingest_financial_ratio(**_: Any) -> dict[str, Any]:
    """
    Bronze financial ratio weekly.

    Loads ticker universe from listing bronze (HOSE/HNX) before ingest — the
    pipeline alone only sees cfg.tickers defaults (~20 symbols) without this.
    """
    _load_dotenv()
    from ingestion.structure_data import (
        configure_logging,
        run_financial_ratio_ingestion_pipeline,
    )

    configure_logging()
    _require_vnstock_api_key()
    cfg = _structured_ingestion_config()
    n = _load_listing_tickers_into_cfg(
        cfg, exchange_filter=cfg.financial_ratio_exchange_filter
    )
    LOGGER.info("Financial ratio bronze: %s tickers (exchange=%s)", n, cfg.financial_ratio_exchange_filter)
    result = run_financial_ratio_ingestion_pipeline(cfg)
    LOGGER.info("Financial ratio bronze finished")
    return result


def silver_dataset(
    dataset: str,
    *,
    run_partition: str | None = None,
    strict: bool = False,
) -> None:
    """Run pipeline.silver.cli for one dataset (CLI accepts single --dataset only)."""
    cmd = [sys.executable, "-m", "pipeline.silver.cli", "--dataset", dataset]
    if run_partition:
        cmd.extend(["--run-partition", run_partition])
    if strict:
        cmd.append("--strict")
    run_subprocess(cmd)


def make_silver_task(dataset: str, *, strict: bool = True):
    """Factory for per-dataset silver PythonOperator callables."""

    def _run(**_: Any) -> None:
        silver_dataset(dataset, strict=strict)

    _run.__name__ = f"silver_{dataset}"
    _run.__doc__ = f"Silver transform: {dataset}"
    return _run


silver_listing = make_silver_task("listing")
silver_company = make_silver_task("company")
silver_price = make_silver_task("price")
silver_index_price = make_silver_task("index_price")
silver_price_board = make_silver_task("price_board")
silver_financial_ratio = make_silver_task("financial_ratio")


def silver_news_from_xcom(**context: Any) -> None:
    """Silver news — run_partition from bronze_news XCom (not Airflow ds)."""
    ti = context["ti"]
    run_partition = ti.xcom_pull(task_ids="bronze_news")
    if not run_partition:
        raise ValueError("Missing XCom from bronze_news")
    silver_dataset("news", run_partition=run_partition, strict=True)


def silver_bctc_from_xcom(**context: Any) -> None:
    """Silver bctc_pdf_meta — run_partition from bronze_bctc XCom."""
    ti = context["ti"]
    run_partition = ti.xcom_pull(task_ids="bronze_bctc")
    if not run_partition:
        raise ValueError("Missing XCom from bronze_bctc")
    silver_dataset("bctc_pdf_meta", run_partition=run_partition)


def load_silver(
    datasets: str | Iterable[str],
    *,
    latest_partitions: int | None = None,
) -> None:
    """Load silver parquet into PostgreSQL (warehouse.loader.cli accepts comma list)."""
    if isinstance(datasets, str):
        dataset_arg = datasets
    else:
        dataset_arg = ",".join(datasets)
    cmd = [
        sys.executable,
        "-m",
        "warehouse.loader.cli",
        "load-silver",
        "--dataset",
        dataset_arg,
    ]
    if latest_partitions is not None:
        cmd.extend(["--latest-partitions", str(latest_partitions)])
    run_subprocess(cmd)


def dbt_select(selectors: str | Iterable[str], *, do_test: bool = False) -> None:
    """dbt run --select with upstream (+mart_x syntax)."""
    if isinstance(selectors, str):
        select_arg = selectors
    else:
        select_arg = " ".join(selectors)
    root = get_repo_root()
    base = [
        "dbt",
        "run",
        "--project-dir",
        DBT_PROJECT_DIR,
        "--profiles-dir",
        DBT_PROFILES_DIR,
        "--select",
        select_arg,
    ]
    run_subprocess(base, cwd=root)
    if do_test:
        run_subprocess(
            [
                "dbt",
                "test",
                "--project-dir",
                DBT_PROJECT_DIR,
                "--profiles-dir",
                DBT_PROFILES_DIR,
                "--select",
                select_arg,
            ],
            cwd=root,
        )


def dbt_run_full(**_: Any) -> None:
    """Full dbt run (gold_full_refresh DAG)."""
    root = get_repo_root()
    run_subprocess(
        [
            "dbt",
            "run",
            "--project-dir",
            DBT_PROJECT_DIR,
            "--profiles-dir",
            DBT_PROFILES_DIR,
            "--full-refresh",
        ],
        cwd=root,
    )


def dbt_test_full(**_: Any) -> None:
    """Full dbt test (gold_full_refresh DAG)."""
    root = get_repo_root()
    run_subprocess(
        [
            "dbt",
            "test",
            "--project-dir",
            DBT_PROJECT_DIR,
            "--profiles-dir",
            DBT_PROFILES_DIR,
        ],
        cwd=root,
    )
