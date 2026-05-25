from __future__ import annotations

import logging
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from .runs_log import write_runs_entry

LOGGER = logging.getLogger(__name__)

INPUT_GLOB = "raw/Structure_Data/financial_ratio/snapshot_date=*/*.parquet"
OUTPUT_PATH = "silver/financial_ratio"
PART_FILENAME = "PART-000.parquet"

ID_VARS = [
    "ticker",
    "item",
    "item_id",
    "ingested_at",
    "data_source",
    "ratio_period",
    "snapshot_date",
    "run_partition",
    "source_file",
]
SILVER_COLUMNS = [
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
]
DEDUP_KEY = ["ticker", "item_code", "period"]

PERIOD_COLUMN_RE = re.compile(r"^\d{4}-(Q\d|year)(\w*)$")
QUARTER_RE = re.compile(r"^(?P<year>\d{4})-Q(?P<quarter>\d)")
ANNUAL_RE = re.compile(r"^(?P<year>\d{4})-year")
MIN_SNAPSHOT_WATERMARK = ""


def _extract_snapshot_partition(path: Path) -> str | None:
    prefix = "snapshot_date="
    for part in path.parts:
        if part.startswith(prefix):
            return part[len(prefix) :]
    return None


def _snapshot_text(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.isoformat()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    return text or None


def _parse_snapshot_date(value: object) -> str | None:
    text = _snapshot_text(value)
    if text is None:
        return None
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return None
    return text


def filter_new_snapshots(
    snapshots: pd.DataFrame,
    last_snapshot_date: object,
) -> pd.DataFrame:
    watermark = _snapshot_text(last_snapshot_date) or MIN_SNAPSHOT_WATERMARK
    snapshot_values = snapshots["snapshot_date"].map(_snapshot_text).fillna("")
    return snapshots.loc[snapshot_values > watermark].copy()


def _compact_text(value: object) -> object:
    if pd.isna(value):
        return pd.NA
    text = re.sub(r"\s+", " ", str(value)).strip()
    if text.lower() in {"", "nan", "none", "null", "<na>"}:
        return pd.NA
    return text


def _decode_period(value: object) -> tuple[object, object, object, object]:
    if value is None or pd.isna(value):
        return pd.NA, pd.NA, pd.NA, pd.NA
    text = str(value).strip()
    quarter_match = QUARTER_RE.match(text)
    if quarter_match:
        year = int(quarter_match.group("year"))
        quarter = int(quarter_match.group("quarter"))
        return f"{year}-Q{quarter}", "quarter", year, quarter

    annual_match = ANNUAL_RE.match(text)
    if annual_match:
        year = int(annual_match.group("year"))
        return str(year), "annual", year, pd.NA

    return pd.NA, pd.NA, pd.NA, pd.NA


class FinancialRatioTransformer:
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        self.input_glob = INPUT_GLOB
        self.output_path = self.base_path / OUTPUT_PATH
        self.part_filename = PART_FILENAME

    def run(self) -> dict:
        started_at = datetime.now(timezone.utc)
        try:
            return self._run_transform(started_at)
        except Exception as exc:
            write_runs_entry(
                dataset="financial_ratio",
                silver_root=self.base_path / "silver",
                run_partition=None,
                started_at=started_at,
                input_rows=0,
                output_rows=0,
                output_path=self.output_path,
                status="error",
                error=str(exc),
            )
            raise

    def _run_transform(self, started_at: datetime) -> dict:
        discovered = self._discover_files()
        watermark = self._get_watermark()
        selected = [
            item for item in discovered if item["snapshot_date"] > watermark
        ]

        if not selected:
            write_runs_entry(
                dataset="financial_ratio",
                silver_root=self.base_path / "silver",
                run_partition=watermark,
                started_at=started_at,
                input_rows=0,
                output_rows=0,
                output_path=self.output_path,
                latest_key=watermark or None,
                status="success",
            )
            return {
                "rows_written": 0,
                "partitions": [],
                "watermark": watermark,
            }

        frames: list[pd.DataFrame] = []
        for item in selected:
            path = item["path"]
            df = pd.read_parquet(path).copy()
            df["snapshot_date"] = item["snapshot_date"]
            df["run_partition"] = item["run_partition"]
            df["source_file"] = self._relative_source_file(path)
            frames.append(df)

        raw = pd.concat(frames, ignore_index=True, sort=False)
        long_df = self._melt(raw)
        parsed = self._parse_period(long_df)
        coerced = self._coerce(parsed)
        deduped = self._dedup(coerced)

        processed_watermark = max(item["snapshot_date"] for item in selected)
        if deduped.empty:
            LOGGER.info(
                "Silver financial_ratio processed no parseable rows through watermark=%s",
                processed_watermark,
            )
            write_runs_entry(
                dataset="financial_ratio",
                silver_root=self.base_path / "silver",
                run_partition=processed_watermark,
                started_at=started_at,
                input_rows=len(raw),
                output_rows=0,
                output_path=self.output_path,
                latest_key=processed_watermark,
                status="success",
            )
            return {
                "rows_written": 0,
                "partitions": [],
                "watermark": processed_watermark,
            }

        rows_written = 0
        partitions: list[str] = []
        for (period_type, year), frame in deduped.groupby(
            ["period_type", "year"],
            sort=True,
            dropna=True,
        ):
            year_int = int(year)
            period_type_text = str(period_type)
            rows_written += self._write_partition(frame, period_type_text, year_int)
            partitions.append(f"period_type={period_type_text}/year={year_int}")

        LOGGER.info(
            "Silver financial_ratio done: rows_written=%s partitions=%s watermark=%s",
            rows_written,
            len(partitions),
            processed_watermark,
        )
        write_runs_entry(
            dataset="financial_ratio",
            silver_root=self.base_path / "silver",
            run_partition=processed_watermark,
            started_at=started_at,
            input_rows=len(raw),
            output_rows=rows_written,
            output_path=self.output_path,
            latest_key=processed_watermark,
            status="success",
        )
        return {
            "rows_written": rows_written,
            "partitions": partitions,
            "watermark": processed_watermark,
        }

    def _discover_files(self) -> list[dict]:
        files: list[dict[str, Any]] = []
        for path in sorted(self.base_path.glob(self.input_glob)):
            if not path.is_file():
                continue
            run_partition = _extract_snapshot_partition(path)
            snapshot_date = _parse_snapshot_date(run_partition)
            if snapshot_date is None or run_partition is None:
                LOGGER.warning("Skipping financial_ratio file with invalid snapshot: %s", path)
                continue
            files.append(
                {
                    "path": path,
                    "snapshot_date": snapshot_date,
                    "run_partition": run_partition,
                }
            )
        return sorted(files, key=lambda item: (item["snapshot_date"], str(item["path"])))

    def _get_watermark(self) -> str:
        if not self.output_path.exists():
            return MIN_SNAPSHOT_WATERMARK

        watermark = MIN_SNAPSHOT_WATERMARK
        for path in sorted(self.output_path.glob("period_type=*/year=*/*.parquet")):
            if not path.is_file():
                continue
            df = pd.read_parquet(path, columns=["snapshot_date"])
            if "snapshot_date" not in df.columns or df.empty:
                continue
            snapshots = df["snapshot_date"].map(_parse_snapshot_date).dropna()
            if snapshots.empty:
                continue
            latest = max(snapshots)
            if latest > watermark:
                watermark = latest
        return watermark

    def _melt(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        for column in ID_VARS:
            if column not in out.columns:
                out[column] = pd.NA

        period_cols = [
            column for column in out.columns if PERIOD_COLUMN_RE.match(str(column))
        ]
        if not period_cols:
            return pd.DataFrame(columns=[*ID_VARS, "period_raw", "value", "_period_col_index"])

        column_indices = {column: index for index, column in enumerate(out.columns)}
        melted = pd.melt(
            out,
            id_vars=ID_VARS,
            value_vars=period_cols,
            var_name="period_raw",
            value_name="value",
        )
        melted["_period_col_index"] = melted["period_raw"].map(column_indices)
        return melted

    def _parse_period(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        if out.empty:
            out["period"] = pd.Series(dtype="string")
            out["period_type"] = pd.Series(dtype="string")
            out["year"] = pd.Series(dtype="Int64")
            out["quarter"] = pd.Series(dtype="Int64")
            return out

        decoded = pd.DataFrame(
            out["period_raw"].map(_decode_period).tolist(),
            index=out.index,
            columns=["period", "period_type", "year", "quarter"],
        )
        out = pd.concat([out, decoded], axis=1)
        out = out.loc[out["period"].notna()].copy()
        if out.empty:
            return out.reset_index(drop=True)

        out["year"] = pd.to_numeric(out["year"], errors="coerce").astype("Int64")
        out["quarter"] = pd.to_numeric(out["quarter"], errors="coerce").astype("Int64")
        period_col_index = (
            out["_period_col_index"]
            if "_period_col_index" in out.columns
            else pd.Series(-1, index=out.index)
        )
        out["_period_col_index"] = pd.to_numeric(
            period_col_index,
            errors="coerce",
        ).fillna(-1)

        duplicate_subset = [column for column in ID_VARS if column in out.columns]
        duplicate_subset.append("period")
        out = (
            out.sort_values("_period_col_index", kind="stable")
            .drop_duplicates(duplicate_subset, keep="last")
            .reset_index(drop=True)
        )
        return out

    def _coerce(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        for column in [
            "ticker",
            "period",
            "period_type",
            "year",
            "quarter",
            "item",
            "item_id",
            "value",
            "data_source",
            "snapshot_date",
            "ingested_at",
            "run_partition",
            "source_file",
        ]:
            if column not in out.columns:
                out[column] = pd.NA

        result = pd.DataFrame(index=out.index)
        result["ticker"] = out["ticker"].map(_compact_text).astype("string").str.upper()
        result["period"] = out["period"].map(_compact_text).astype("string")
        result["period_type"] = out["period_type"].map(_compact_text).astype("string")
        result["year"] = pd.to_numeric(out["year"], errors="coerce").astype("Int64")
        result["quarter"] = pd.to_numeric(out["quarter"], errors="coerce").astype("Int64")
        result["item_code"] = out["item_id"].map(_compact_text).astype("string")
        result["item_name"] = out["item"].map(_compact_text).astype("string")
        result["value"] = pd.to_numeric(out["value"], errors="coerce").astype(float)
        result["source"] = out["data_source"].map(_compact_text).astype("string")
        result["snapshot_date"] = out["snapshot_date"].map(_snapshot_text).astype(object)
        result["fetched_at"] = pd.to_datetime(
            out["ingested_at"],
            errors="coerce",
            utc=True,
        )
        result["run_partition"] = out["run_partition"].map(_compact_text).astype("string")
        result["source_file"] = out["source_file"].map(_compact_text).astype("string")
        return result[SILVER_COLUMNS].reset_index(drop=True)

    def _dedup(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        for column in SILVER_COLUMNS:
            if column not in out.columns:
                out[column] = pd.NA
        if out.empty:
            return out[SILVER_COLUMNS].reset_index(drop=True)

        out["_snapshot_sort"] = out["snapshot_date"].map(_snapshot_text).fillna("")
        out["_source_file_sort"] = out["source_file"].astype("string").fillna("")
        out = out.sort_values(
            ["_snapshot_sort", "_source_file_sort"],
            kind="stable",
        ).drop_duplicates(DEDUP_KEY, keep="last")
        return out[SILVER_COLUMNS].reset_index(drop=True)

    def _write_partition(self, df: pd.DataFrame, period_type: str, year: int) -> int:
        partition_dir = (
            self.output_path / f"period_type={period_type}" / f"year={int(year)}"
        )
        output_file = partition_dir / self.part_filename

        frames = [df.copy()]
        if output_file.exists():
            frames.insert(0, pd.read_parquet(output_file))

        combined = pd.concat(frames, ignore_index=True, sort=False)
        combined = self._dedup(combined)
        combined["year"] = pd.to_numeric(combined["year"], errors="coerce").astype("Int64")
        combined = combined.loc[
            combined["period_type"].astype("string").eq(period_type)
            & combined["year"].eq(int(year))
        ].copy()
        combined = combined[SILVER_COLUMNS].reset_index(drop=True)

        partition_dir.mkdir(parents=True, exist_ok=True)
        for old_file in partition_dir.glob("PART-*.parquet"):
            if old_file != output_file:
                old_file.unlink()
        combined.to_parquet(output_file, engine="pyarrow", index=False)
        LOGGER.info("Wrote Silver financial_ratio partition: rows=%s path=%s", len(combined), output_file)
        return len(combined)

    def _relative_source_file(self, path: Path) -> str:
        try:
            return path.resolve().relative_to(self.base_path.resolve()).as_posix()
        except ValueError:
            return path.as_posix()
