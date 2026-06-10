from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from warehouse.loader.silver_loader import DATASET_CONFIG, DATASET_ORDER, load_dataset

DEFAULT_DATABASE_URL = "postgresql://stock:stock@localhost:55432/stock_pipeline"


def _load_dotenv() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    load_dotenv(Path(__file__).resolve().parents[2] / ".env", override=False)


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )


def parse_datasets(value: str) -> list[str]:
    raw = (value or "").strip()
    if raw == "all":
        return list(DATASET_ORDER)
    datasets = [part.strip() for part in raw.split(",") if part.strip()]
    unknown = [dataset for dataset in datasets if dataset not in DATASET_CONFIG]
    if unknown:
        raise ValueError(
            f"Invalid dataset: {unknown}. Valid values: {list(DATASET_CONFIG)} or all"
        )
    if not datasets:
        raise ValueError("Dataset must not be empty")
    return datasets


def get_connection():
    try:
        import psycopg2
    except ImportError as exc:
        raise SystemExit(
            "psycopg2 is required for PostgreSQL loading. "
            "Install dependencies with `pip install -r requirements.txt`."
        ) from exc
    _load_dotenv()
    url = os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)
    logging.info("Connecting to DB: %s", url)
    return psycopg2.connect(url)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="warehouse.loader.cli")
    subparsers = parser.add_subparsers(dest="command")
    load_parser = subparsers.add_parser("load-silver", help="Load Silver parquet into PostgreSQL")
    load_parser.add_argument(
        "--dataset",
        default="all",
        help="Dataset to load: all | price | news | price,index_price | listing,company",
    )
    load_parser.add_argument(
        "--latest-partitions",
        type=int,
        default=None,
        help=(
            "Load only the latest N filesystem partitions for datasets that support it "
            "(currently price, index_price, price_board). Omit for full load."
        ),
    )
    return parser


def cmd_load_silver(args: argparse.Namespace) -> int:
    try:
        datasets = parse_datasets(args.dataset)
    except ValueError as exc:
        logging.error("%s", exc)
        return 1

    failed: list[str] = []
    conn = get_connection()
    try:
        for dataset in datasets:
            if not load_dataset(
                conn,
                dataset,
                latest_partitions=args.latest_partitions,
            ):
                failed.append(dataset)
    finally:
        conn.close()

    if failed:
        logging.error("Failed datasets: %s", failed)
        return 1
    logging.info("All datasets loaded successfully.")
    return 0


def main() -> None:
    configure_logging()
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "load-silver":
        raise SystemExit(cmd_load_silver(args))
    parser.print_help()
    raise SystemExit(1)


if __name__ == "__main__":
    main()
