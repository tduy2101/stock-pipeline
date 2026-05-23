from __future__ import annotations

import glob
import logging
import math
from datetime import date, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

try:
    import psycopg2.extras as pg_extras
    from psycopg2 import sql as pg_sql
except ImportError:  # pragma: no cover - exercised in environments without DB deps.
    pg_extras = None
    pg_sql = None

LOGGER = logging.getLogger(__name__)

DATASET_ORDER = [
    "price",
    "index_price",
    "listing",
    "company",
    "financial_ratio",
    "price_board",
]

DATASET_CONFIG: dict[str, dict[str, Any]] = {
    "price": {
        "glob": "data-lake/silver/price/**/*.parquet",
        "table": "silver.price",
        "key_cols": ["ticker", "trading_date"],
        "columns": [
            "ticker",
            "trading_date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "value",
            "value_is_derived",
            "source",
            "instrument_type",
            "fetched_at",
            "is_suspicious",
            "bronze_ingested_at",
            "run_partition",
            "source_file",
        ],
        "date_cols": ["trading_date"],
        "timestamp_cols": ["fetched_at"],
        "text_cols": ["ticker", "source", "instrument_type", "bronze_ingested_at", "run_partition", "source_file"],
    },
    "index_price": {
        "glob": "data-lake/silver/index_price/**/*.parquet",
        "table": "silver.index_price",
        "key_cols": ["index_code", "trading_date"],
        "columns": [
            "index_code",
            "trading_date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "value",
            "value_is_derived",
            "source",
            "instrument_type",
            "fetched_at",
            "is_suspicious",
            "bronze_ingested_at",
            "run_partition",
            "source_file",
        ],
        "date_cols": ["trading_date"],
        "timestamp_cols": ["fetched_at"],
        "text_cols": ["index_code", "source", "instrument_type", "bronze_ingested_at", "run_partition", "source_file"],
    },
    "listing": {
        "glob": "data-lake/silver/listing/**/*.parquet",
        "table": "silver.listing",
        "key_cols": ["symbol"],
        "columns": [
            "symbol",
            "organ_name",
            "en_organ_name",
            "exchange",
            "security_type",
            "source",
            "crawled_at",
            "run_partition",
            "source_file",
        ],
        "date_cols": [],
        "timestamp_cols": ["crawled_at"],
        "text_cols": ["symbol", "organ_name", "en_organ_name", "exchange", "security_type", "source", "run_partition", "source_file"],
    },
    "company": {
        "glob": "data-lake/silver/company/**/*.parquet",
        "table": "silver.company",
        "key_cols": ["ticker"],
        "columns": [
            "ticker",
            "symbol",
            "exchange",
            "company_type",
            "business_model",
            "founded_date",
            "listing_date",
            "charter_capital",
            "number_of_employees",
            "par_value",
            "listing_price",
            "listed_volume",
            "outstanding_shares",
            "as_of_date",
            "ceo_name",
            "ceo_position",
            "auditor",
            "address",
            "phone",
            "fax",
            "email",
            "website",
            "branches",
            "history",
            "free_float_percentage",
            "free_float",
            "inspector_name",
            "inspector_position",
            "establishment_license",
            "business_code",
            "tax_id",
            "source",
            "company_method",
            "snapshot_date",
            "fetched_at",
            "run_partition",
            "source_file",
        ],
        "date_cols": ["founded_date", "listing_date", "as_of_date"],
        "timestamp_cols": ["fetched_at"],
        "text_cols": [
            "ticker",
            "symbol",
            "exchange",
            "company_type",
            "business_model",
            "ceo_name",
            "ceo_position",
            "auditor",
            "address",
            "phone",
            "fax",
            "email",
            "website",
            "branches",
            "history",
            "inspector_name",
            "inspector_position",
            "establishment_license",
            "business_code",
            "tax_id",
            "source",
            "company_method",
            "snapshot_date",
            "run_partition",
            "source_file",
        ],
    },
    "financial_ratio": {
        "glob": "data-lake/silver/financial_ratio/**/*.parquet",
        "table": "silver.financial_ratio",
        "key_cols": ["ticker", "item_code", "period"],
        "columns": [
            "ticker",
            "period",
            "period_type",
            "year",
            "quarter",
            "item_code",
            "item_name",
            "value",
            "source",
            "snapshot_date",
            "fetched_at",
            "run_partition",
            "source_file",
        ],
        "date_cols": [],
        "timestamp_cols": ["fetched_at"],
        "text_cols": ["ticker", "period", "period_type", "item_code", "item_name", "source", "snapshot_date", "run_partition", "source_file"],
    },
    "price_board": {
        "glob": "data-lake/silver/price_board/**/*.parquet",
        "table": "silver.price_board",
        "key_cols": ["symbol", "trading_date"],
        "columns": [
            "symbol",
            "trading_date",
            "exchange",
            "ceiling_price",
            "floor_price",
            "reference_price",
            "open_price",
            "high_price",
            "low_price",
            "close_price",
            "average_price",
            "volume_accumulated",
            "total_value",
            "price_change",
            "percent_change",
            "bid_price_1",
            "bid_vol_1",
            "bid_price_2",
            "bid_vol_2",
            "bid_price_3",
            "bid_vol_3",
            "ask_price_1",
            "ask_vol_1",
            "ask_price_2",
            "ask_vol_2",
            "ask_price_3",
            "ask_vol_3",
            "foreign_buy_volume",
            "foreign_sell_volume",
            "foreign_room",
            "source",
            "snapshot_at",
            "is_suspicious",
            "run_partition",
            "source_file",
        ],
        "date_cols": ["trading_date"],
        "timestamp_cols": ["snapshot_at"],
        "text_cols": ["symbol", "exchange", "source", "run_partition", "source_file"],
    },
}


def _is_nullish(value: Any) -> bool:
    if value is None:
        return True
    if value is pd.NA or value is pd.NaT:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _text_or_none(value: Any) -> str | None:
    if _is_nullish(value):
        return None
    return str(value)


def _object_with_none(series: pd.Series) -> pd.Series:
    out = series.astype(object)
    return out.where(pd.notna(out), None)


def _to_python(value: Any) -> Any:
    if _is_nullish(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return float(value)
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, (date, datetime, bool, int, float, str)):
        return value
    return value


def _validate_key_quality(df: pd.DataFrame, dataset: str, key_cols: list[str]) -> None:
    missing = [col for col in key_cols if col not in df.columns]
    if missing:
        raise ValueError(f"[{dataset}] Missing key columns: {missing}")
    null_key = df[key_cols].isna().any(axis=1)
    if null_key.any():
        raise ValueError(
            f"[{dataset}] Found {int(null_key.sum())} rows with null key values: {key_cols}"
        )
    duplicate_key = df.duplicated(subset=key_cols)
    if duplicate_key.any():
        raise ValueError(
            f"[{dataset}] Found {int(duplicate_key.sum())} duplicate key rows: {key_cols}"
        )


def prepare_dataframe(df: pd.DataFrame, dataset: str) -> pd.DataFrame:
    if dataset not in DATASET_CONFIG:
        raise ValueError(f"Unsupported dataset: {dataset}")

    cfg = DATASET_CONFIG[dataset]
    key_cols = list(cfg["key_cols"])
    columns = list(cfg["columns"])
    date_cols = set(cfg.get("date_cols", []))
    timestamp_cols = set(cfg.get("timestamp_cols", []))
    text_cols = set(cfg.get("text_cols", []))

    missing_keys = [col for col in key_cols if col not in df.columns]
    if missing_keys:
        raise ValueError(f"[{dataset}] Missing key columns: {missing_keys}")

    out = df.copy()
    for column in columns:
        if column not in out.columns:
            out[column] = None
    out = out[columns].copy()

    for column in columns:
        if column in date_cols:
            parsed = pd.to_datetime(out[column], errors="coerce", utc=True)
            out[column] = _object_with_none(parsed.dt.date)
        elif column in timestamp_cols:
            parsed = pd.to_datetime(out[column], errors="coerce", utc=True)
            out[column] = _object_with_none(parsed)
        elif column in text_cols:
            out[column] = out[column].map(_text_or_none).astype(object)
        elif pd.api.types.is_extension_array_dtype(out[column]):
            out[column] = _object_with_none(out[column])
        elif pd.api.types.is_float_dtype(out[column]):
            out[column] = _object_with_none(out[column])
        elif pd.api.types.is_bool_dtype(out[column]):
            out[column] = _object_with_none(out[column])
        elif pd.api.types.is_datetime64_any_dtype(out[column]):
            out[column] = _object_with_none(pd.to_datetime(out[column], errors="coerce"))
        else:
            out[column] = _object_with_none(out[column])

    if dataset == "financial_ratio":
        for column in ("year", "quarter"):
            out[column] = out[column].map(_to_python).astype(object)

    _validate_key_quality(out, dataset, key_cols)
    return out


def _qualified_table(table: str) -> tuple[str, str]:
    parts = table.split(".", 1)
    if len(parts) != 2 or not all(parts):
        raise ValueError(f"Expected schema-qualified table name, got: {table}")
    return parts[0], parts[1]


def upsert_table(
    conn,
    table: str,
    df: pd.DataFrame,
    key_cols: list[str],
    batch_size: int = 2000,
) -> dict[str, int]:
    if pg_extras is None or pg_sql is None:
        raise ImportError(
            "psycopg2 is required for PostgreSQL loading. "
            "Install dependencies with `pip install -r requirements.txt`."
        )
    if df.empty:
        return {"rows_read": 0, "rows_inserted": 0, "rows_updated": 0}

    columns = list(df.columns)
    update_cols = [column for column in columns if column not in key_cols]
    if not update_cols:
        raise ValueError(f"[{table}] No non-key columns available for update")

    schema, table_name = _qualified_table(table)
    insert_sql = pg_sql.SQL(
        """
        INSERT INTO {table} ({columns})
        VALUES %s
        ON CONFLICT ({key_columns})
        DO UPDATE SET {updates}
        RETURNING (xmax = 0) AS is_insert
        """
    ).format(
        table=pg_sql.Identifier(schema, table_name),
        columns=pg_sql.SQL(", ").join(pg_sql.Identifier(column) for column in columns),
        key_columns=pg_sql.SQL(", ").join(pg_sql.Identifier(column) for column in key_cols),
        updates=pg_sql.SQL(", ").join(
            pg_sql.SQL("{column} = EXCLUDED.{column}").format(
                column=pg_sql.Identifier(column)
            )
            for column in update_cols
        ),
    )
    template = "(" + ", ".join(["%s"] * len(columns)) + ")"

    rows_inserted = 0
    rows_updated = 0
    with conn.cursor() as cursor:
        query = insert_sql.as_string(cursor)
        for start in range(0, len(df), batch_size):
            batch = df.iloc[start : start + batch_size]
            records = [
                tuple(_to_python(value) for value in row)
                for row in batch.itertuples(index=False, name=None)
            ]
            results = pg_extras.execute_values(
                cursor,
                query,
                records,
                template=template,
                page_size=batch_size,
                fetch=True,
            )
            rows_inserted += sum(1 for row in results if row[0])
            rows_updated += sum(1 for row in results if not row[0])

    conn.commit()
    return {
        "rows_read": int(len(df)),
        "rows_inserted": int(rows_inserted),
        "rows_updated": int(rows_updated),
    }


def write_audit(
    conn,
    dataset: str,
    run_partition: str | None,
    rows_read: int,
    rows_inserted: int,
    rows_updated: int,
    status: str,
    error_msg: str | None = None,
) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO silver.load_audit
              (dataset, run_partition, rows_read, rows_inserted, rows_updated, status, error_msg)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                dataset,
                run_partition,
                int(rows_read),
                int(rows_inserted),
                int(rows_updated),
                status,
                error_msg,
            ),
        )
    conn.commit()


def _read_parquet_files(pattern: str) -> tuple[pd.DataFrame, list[str]]:
    files = sorted(glob.glob(pattern, recursive=True))
    if not files:
        return pd.DataFrame(), []
    frames = [pd.read_parquet(file) for file in files]
    return pd.concat(frames, ignore_index=True, sort=False), files


def _run_partition_for_audit(df: pd.DataFrame) -> str | None:
    if "run_partition" not in df.columns or df.empty:
        return None
    values = [str(value) for value in df["run_partition"].dropna().unique()]
    if not values:
        return None
    return max(values)


def load_dataset(conn, dataset: str) -> bool:
    if dataset not in DATASET_CONFIG:
        raise ValueError(f"Unsupported dataset: {dataset}")

    cfg = DATASET_CONFIG[dataset]
    rows_read = rows_inserted = rows_updated = 0
    run_partition: str | None = None

    try:
        raw_df, files = _read_parquet_files(cfg["glob"])
        if not files:
            raise FileNotFoundError(f"No parquet files found for {dataset}: {cfg['glob']}")

        LOGGER.info("[%s] Read %s rows from %s parquet files", dataset, len(raw_df), len(files))
        prepared = prepare_dataframe(raw_df, dataset)
        run_partition = _run_partition_for_audit(prepared)
        result = upsert_table(conn, cfg["table"], prepared, list(cfg["key_cols"]))
        rows_read = result["rows_read"]
        rows_inserted = result["rows_inserted"]
        rows_updated = result["rows_updated"]
        write_audit(
            conn,
            dataset,
            run_partition,
            rows_read,
            rows_inserted,
            rows_updated,
            "success",
        )
        LOGGER.info(
            "[%s] rows_read=%s inserted=%s updated=%s",
            dataset,
            rows_read,
            rows_inserted,
            rows_updated,
        )
        print(f"[{dataset}] rows_read={rows_read} inserted={rows_inserted} updated={rows_updated}")
        return True
    except Exception as exc:
        LOGGER.error("[%s] load failed: %s", dataset, exc, exc_info=True)
        try:
            conn.rollback()
            write_audit(
                conn,
                dataset,
                run_partition,
                rows_read,
                rows_inserted,
                rows_updated,
                "error",
                str(exc),
            )
        except Exception:
            LOGGER.exception("[%s] failed to write error audit", dataset)
        return False


def parquet_row_count(dataset: str) -> int:
    if dataset not in DATASET_CONFIG:
        raise ValueError(f"Unsupported dataset: {dataset}")
    total = 0
    for file in sorted(glob.glob(DATASET_CONFIG[dataset]["glob"], recursive=True)):
        total += len(pd.read_parquet(file))
    return int(total)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]
