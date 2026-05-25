from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from .runs_log import write_runs_entry

LOGGER = logging.getLogger(__name__)

INPUT_GLOB = "raw/Structure_Data/price_board/snapshot_at=*/PRICE_BOARD_SNAPSHOT.parquet"
OUTPUT_PATH = "silver/price_board"
PART_FILENAME = "PART-000.parquet"

PRICE_COLUMNS = [
    "ceiling_price",
    "floor_price",
    "reference_price",
    "open_price",
    "high_price",
    "low_price",
    "close_price",
    "average_price",
    "price_change",
    "bid_price_1",
    "bid_price_2",
    "bid_price_3",
    "ask_price_1",
    "ask_price_2",
    "ask_price_3",
]
FLOAT_COLUMNS = [*PRICE_COLUMNS, "total_value", "percent_change"]
INT_COLUMNS = [
    "volume_accumulated",
    "bid_vol_1",
    "bid_vol_2",
    "bid_vol_3",
    "ask_vol_1",
    "ask_vol_2",
    "ask_vol_3",
    "foreign_buy_volume",
    "foreign_sell_volume",
    "foreign_room",
]
SILVER_COLUMNS = [
    "symbol",
    "trading_date",
    "exchange",
    "reference_price",
    "ceiling_price",
    "floor_price",
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
    "bid_price_2",
    "bid_price_3",
    "bid_vol_1",
    "bid_vol_2",
    "bid_vol_3",
    "ask_price_1",
    "ask_price_2",
    "ask_price_3",
    "ask_vol_1",
    "ask_vol_2",
    "ask_vol_3",
    "foreign_buy_volume",
    "foreign_sell_volume",
    "foreign_room",
    "source",
    "snapshot_at",
    "run_partition",
    "source_file",
    "is_suspicious",
]
DEDUP_KEY = ["symbol", "trading_date"]


def _extract_snapshot_partition(path: Path) -> str | None:
    prefix = "snapshot_at="
    for part in path.parts:
        if part.startswith(prefix):
            return part[len(prefix) :]
    return None


def _compact_text(value: object) -> object:
    if value is None or pd.isna(value):
        return pd.NA
    text = " ".join(str(value).strip().split())
    if text.lower() in {"", "nan", "none", "null", "<na>"}:
        return pd.NA
    return text


def _parse_date_text(value: object) -> date | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        pass
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return None
    return pd.Timestamp(parsed).date()


def dedupe_to_daily_latest(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for column in DEDUP_KEY:
        if column not in out.columns:
            out[column] = pd.NA
    if "snapshot_at" not in out.columns:
        out["snapshot_at"] = pd.NaT
    if out.empty:
        return out.reset_index(drop=True)

    out["_snapshot_sort"] = pd.to_datetime(
        out["snapshot_at"],
        errors="coerce",
        utc=True,
    )
    out = (
        out.sort_values(
            "_snapshot_sort",
            ascending=False,
            kind="stable",
            na_position="last",
        )
        .drop_duplicates(subset=DEDUP_KEY, keep="first")
        .drop(columns=["_snapshot_sort"])
        .reset_index(drop=True)
    )
    return out


class PriceBoardTransformer:
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
                dataset="price_board",
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
            item for item in discovered if item["snapshot_at"].date() >= watermark
        ]

        if not selected:
            watermark_text = watermark.isoformat()
            write_runs_entry(
                dataset="price_board",
                silver_root=self.base_path / "silver",
                run_partition=watermark_text,
                started_at=started_at,
                input_rows=0,
                output_rows=0,
                output_path=self.output_path,
                latest_key=watermark_text,
                status="success",
            )
            return {
                "rows_written": 0,
                "partitions": [],
                "watermark": watermark_text,
            }

        frames: list[pd.DataFrame] = []
        for item in selected:
            path = item["path"]
            df = pd.read_parquet(path).copy()
            df["snapshot_at"] = item["snapshot_at"]
            df["run_partition"] = item["run_partition"]
            df["source_file"] = self._relative_source_file(path)
            frames.append(df)

        raw = pd.concat(frames, ignore_index=True, sort=False)
        coerced = self._coerce(raw)
        flagged = self._dq_flags(coerced)
        deduped = self._dedup(flagged)

        processed_watermark = max(item["snapshot_at"].date() for item in selected)
        if deduped.empty:
            LOGGER.info(
                "Silver price_board processed no rows through watermark=%s",
                processed_watermark,
            )
            watermark_text = processed_watermark.isoformat()
            write_runs_entry(
                dataset="price_board",
                silver_root=self.base_path / "silver",
                run_partition=watermark_text,
                started_at=started_at,
                input_rows=len(raw),
                output_rows=0,
                output_path=self.output_path,
                latest_key=watermark_text,
                status="success",
            )
            return {
                "rows_written": 0,
                "partitions": [],
                "watermark": watermark_text,
            }

        rows_written = 0
        partitions: list[str] = []
        for trading_date, frame in deduped.groupby(
            "trading_date",
            sort=True,
            dropna=True,
        ):
            date_text = pd.Timestamp(trading_date).date().isoformat()
            rows_written += self._write_partition(frame, date_text)
            partitions.append(date_text)

        LOGGER.info(
            "Silver price_board done: rows_written=%s partitions=%s watermark=%s",
            rows_written,
            len(partitions),
            processed_watermark,
        )
        watermark_text = processed_watermark.isoformat()
        write_runs_entry(
            dataset="price_board",
            silver_root=self.base_path / "silver",
            run_partition=watermark_text,
            started_at=started_at,
            input_rows=len(raw),
            output_rows=rows_written,
            output_path=self.output_path,
            latest_key=watermark_text,
            status="success",
        )
        return {
            "rows_written": rows_written,
            "partitions": partitions,
            "watermark": watermark_text,
        }

    def _discover_files(self) -> list[dict]:
        files: list[dict[str, Any]] = []
        for path in sorted(self.base_path.glob(self.input_glob)):
            if not path.is_file():
                continue
            run_partition = _extract_snapshot_partition(path)
            if run_partition is None:
                LOGGER.warning("Skipping price_board file without snapshot partition: %s", path)
                continue
            try:
                snapshot_at = self._parse_snapshot_at(run_partition)
            except ValueError:
                LOGGER.warning("Skipping price_board file with invalid snapshot: %s", path)
                continue
            files.append(
                {
                    "path": path,
                    "snapshot_at": snapshot_at,
                    "run_partition": run_partition,
                }
            )
        return sorted(files, key=lambda item: (item["snapshot_at"], str(item["path"])))

    def _get_watermark(self) -> date:
        if not self.output_path.exists():
            return date.min

        watermark = date.min
        for partition_dir in sorted(self.output_path.glob("trading_date=*")):
            if not partition_dir.is_dir() or not any(partition_dir.glob("*.parquet")):
                continue
            raw_date = partition_dir.name.removeprefix("trading_date=")
            parsed_date = _parse_date_text(raw_date)
            if parsed_date is not None and parsed_date > watermark:
                watermark = parsed_date

        if watermark > date.min:
            return watermark

        for path in sorted(self.output_path.glob("trading_date=*/*.parquet")):
            if not path.is_file():
                continue
            try:
                df = pd.read_parquet(path, columns=["trading_date"])
            except (KeyError, ValueError):
                continue
            if "trading_date" not in df.columns or df.empty:
                continue
            parsed = pd.to_datetime(df["trading_date"], errors="coerce").dropna()
            if parsed.empty:
                continue
            latest = pd.Timestamp(parsed.max()).date()
            if latest > watermark:
                watermark = latest
        return watermark

    def _parse_snapshot_at(self, raw: str) -> datetime:
        text = str(raw).strip()
        if not text:
            raise ValueError("empty snapshot_at partition")
        if "T" in text:
            date_part, time_part = text.split("T", 1)
            text = f"{date_part}T{time_part.replace('-', ':')}"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as ex:
            raise ValueError(f"invalid snapshot_at partition: {raw}") from ex
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed

    def _coerce(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        for column in [
            "symbol",
            "exchange",
            "data_source",
            "snapshot_at",
            "run_partition",
            "source_file",
            *FLOAT_COLUMNS,
            *INT_COLUMNS,
        ]:
            if column not in out.columns:
                out[column] = pd.NA

        result = pd.DataFrame(index=out.index)
        result["symbol"] = (
            out["symbol"].map(_compact_text).astype("string").str.upper()
        )
        exchange = out["exchange"].map(_compact_text).astype("string").str.upper()
        result["exchange"] = exchange.fillna("UNKNOWN")

        for column in FLOAT_COLUMNS:
            result[column] = pd.to_numeric(out[column], errors="coerce").astype(float)
        for column in INT_COLUMNS:
            result[column] = pd.to_numeric(out[column], errors="coerce").astype("Int64")

        result["source"] = out["data_source"].map(_compact_text).astype("string")
        result["snapshot_at"] = pd.to_datetime(
            out["snapshot_at"],
            errors="coerce",
            utc=True,
        )
        result["trading_date"] = result["snapshot_at"].dt.date
        result["run_partition"] = out["run_partition"].map(_compact_text).astype("string")
        result["source_file"] = out["source_file"].map(_compact_text).astype("string")
        return result[[column for column in SILVER_COLUMNS if column != "is_suspicious"]]

    def _dq_flags(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        for column in ["close_price", "high_price", "low_price", "ceiling_price", "floor_price"]:
            if column not in out.columns:
                out[column] = pd.NA
        if "volume_accumulated" not in out.columns:
            out["volume_accumulated"] = pd.NA

        suspicious = (
            (out["close_price"] <= 0).fillna(False)
            | (out["high_price"] < out["low_price"]).fillna(False)
            | (out["ceiling_price"] < out["floor_price"]).fillna(False)
            | (out["volume_accumulated"] < 0).fillna(False)
        )
        out["is_suspicious"] = suspicious.astype(bool)
        return out[SILVER_COLUMNS].reset_index(drop=True)

    def _dedup(self, df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        for column in SILVER_COLUMNS:
            if column not in out.columns:
                out[column] = pd.NA
        if out.empty:
            return out[SILVER_COLUMNS].reset_index(drop=True)

        out = dedupe_to_daily_latest(out)
        return out[SILVER_COLUMNS].reset_index(drop=True)

    def _write_partition(self, df: pd.DataFrame, trading_date: str) -> int:
        partition_dir = self.output_path / f"trading_date={trading_date}"
        output_file = partition_dir / self.part_filename

        frames = [df.copy()]
        if output_file.exists():
            frames.insert(0, pd.read_parquet(output_file))

        combined = pd.concat(frames, ignore_index=True, sort=False)
        combined = self._dedup(combined)
        target_date = date.fromisoformat(trading_date)
        combined_dates = pd.to_datetime(
            combined["trading_date"],
            errors="coerce",
        ).dt.date
        combined = combined.loc[combined_dates.eq(target_date)].copy()
        combined = combined[SILVER_COLUMNS].reset_index(drop=True)

        partition_dir.mkdir(parents=True, exist_ok=True)
        for old_file in partition_dir.glob("PART-*.parquet"):
            if old_file != output_file:
                old_file.unlink()
        combined.to_parquet(output_file, engine="pyarrow", index=False)
        LOGGER.info("Wrote Silver price_board partition: rows=%s path=%s", len(combined), output_file)
        return len(combined)

    def _relative_source_file(self, path: Path) -> str:
        try:
            return path.resolve().relative_to(self.base_path.resolve()).as_posix()
        except ValueError:
            return path.as_posix()
