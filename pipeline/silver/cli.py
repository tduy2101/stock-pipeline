from __future__ import annotations

import argparse
import logging
import sys

from ingestion.structure_data.common import write_raw_watermark

from .bronze_reader import SilverWriteResult
from .bctc_pdf_meta_transformer import run_bctc_pdf_meta_silver
from .config import SilverConfig
from .financial_ratio_transformer import FinancialRatioTransformer
from .news_transformer import run_news_silver
from .price_board_transformer import PriceBoardTransformer
from .price_transformer import run_index_price_silver, run_price_silver
from .structure_transformer import (
    run_company_silver,
    run_listing_silver,
)

STRUCTURED_DATASETS = [
    "price",
    "index_price",
    "listing",
    "company",
    "financial_ratio",
    "price_board",
]
WATERMARK_DATASETS = {"price": "price", "index_price": "index"}


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run local Bronze-to-Silver transforms.")
    parser.add_argument(
        "--dataset",
        choices=[
            "all",
            "price",
            "index",
            "index_price",
            "listing",
            "company",
            "financial_ratio",
            "price_board",
            "news",
            "bctc_pdf_meta",
        ],
        default="all",
    )
    parser.add_argument("--run-partition", default=None)
    parser.add_argument("--price-run-partition", default=None)
    parser.add_argument("--index-run-partition", default=None)
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail news transform on validation errors (ERROR: messages).",
    )
    return parser


def _update_watermark_after_success(
    cfg: SilverConfig,
    dataset: str,
    result: SilverWriteResult,
) -> None:
    raw_dataset = WATERMARK_DATASETS.get(dataset)
    if not raw_dataset or not result.trading_date_partitions:
        return
    latest_trading_date = max(result.trading_date_partitions)
    write_raw_watermark(
        cfg.structure_bronze_root,
        raw_dataset,
        latest_trading_date,
        run_id=result.run_partition or latest_trading_date,
    )


def _run_dataset(
    dataset: str,
    args: argparse.Namespace,
    cfg: SilverConfig,
) -> SilverWriteResult | dict:
    if dataset == "price":
        result = run_price_silver(
            cfg,
            run_partition=args.price_run_partition or args.run_partition,
        )
    elif dataset in {"index", "index_price"}:
        result = run_index_price_silver(
            cfg,
            run_partition=args.index_run_partition or args.run_partition,
        )
    elif dataset == "listing":
        result = run_listing_silver(cfg)
    elif dataset == "company":
        result = run_company_silver(cfg)
    elif dataset == "financial_ratio":
        result = FinancialRatioTransformer(str(cfg.data_lake_root)).run()
    elif dataset == "price_board":
        result = PriceBoardTransformer(str(cfg.data_lake_root)).run()
    elif dataset == "news":
        if not args.run_partition:
            raise SystemExit("--run-partition is required for --dataset news")
        result = run_news_silver(
            cfg,
            run_partition=args.run_partition,
            strict=args.strict,
        )
    elif dataset == "bctc_pdf_meta":
        if not args.run_partition:
            raise SystemExit("--run-partition is required for --dataset bctc_pdf_meta")
        result = run_bctc_pdf_meta_silver(cfg, run_partition=args.run_partition)
    else:
        raise ValueError(f"Unsupported dataset: {dataset}")

    if isinstance(result, SilverWriteResult):
        _update_watermark_after_success(cfg, result.dataset, result)
    return result


def _log_result(name: str, result: SilverWriteResult | dict | str) -> None:
    if isinstance(result, str):
        logging.info("Silver %s: %s", name, result)
        return
    if isinstance(result, dict):
        logging.info("Silver %s done: %s", name, result)
        return
    logging.info(
        "Silver %s done: input_rows=%s output_rows=%s files=%s output=%s",
        name,
        result.input_rows,
        result.output_rows,
        result.input_files,
        result.output_path,
    )


def main() -> None:
    _configure_logging()
    args = build_parser().parse_args()
    cfg = SilverConfig()

    if args.dataset == "all":
        results: dict[str, SilverWriteResult | str] = {}
        for dataset in STRUCTURED_DATASETS:
            try:
                results[dataset] = _run_dataset(dataset, args, cfg)
            except Exception as ex:
                logging.error("Silver %s failed: %s", dataset, ex)
                results[dataset] = f"FAILED: {ex}"
                continue
    else:
        try:
            result = _run_dataset(args.dataset, args, cfg)
            if isinstance(result, SilverWriteResult):
                results = {result.dataset: result}
            else:
                results = {args.dataset: result}
        except Exception as ex:
            logging.error("Silver %s failed: %s", args.dataset, ex)
            results = {args.dataset: f"FAILED: {ex}"}

    for name, result in results.items():
        _log_result(name, result)

    if any(isinstance(status, str) and status.startswith("FAILED:") for status in results.values()):
        sys.exit(1)


if __name__ == "__main__":
    main()
