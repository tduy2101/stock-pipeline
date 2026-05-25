from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from .config import SilverConfig

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class BronzeBatch:
    dataset: str
    dataframe: pd.DataFrame
    files: list[Path] = field(default_factory=list)
    run_partition: str | None = None
    partitions: list[str] = field(default_factory=list)


@dataclass(slots=True)
class SilverWriteResult:
    dataset: str
    output_path: Path
    input_rows: int
    output_rows: int
    input_files: int
    run_partition: str | None = None
    dq_errors: list[str] = field(default_factory=list)
    dq_warnings: list[str] = field(default_factory=list)
    output_paths: list[Path] = field(default_factory=list)
    trading_date_partitions: list[str] = field(default_factory=list)


def _parse_date_text(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date().isoformat()


def _parse_month_text(value: object) -> str | None:
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    if len(text) == 7 and text[4] == "-":
        text = f"{text}-01"
    parsed = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed):
        return None
    return pd.Timestamp(parsed).strftime("%Y-%m")


def _month_dir(category_dir: Path, month_partition: str) -> Path:
    year, month = month_partition.split("-", 1)
    return category_dir / f"year={year}" / f"month={month}"


def _find_trading_date_column(df: pd.DataFrame) -> object | None:
    lookup = {
        str(c).strip().lower().replace("_", "").replace(" ", ""): c
        for c in df.columns
    }
    return next(
        (lookup[key] for key in ("tradingdate", "time", "date") if key in lookup),
        None,
    )


def extract_partition_value(path: Path, key: str = "trading_date") -> str | None:
    prefix = f"{key}="
    for part in path.parts:
        if part.startswith(prefix):
            return part[len(prefix) :]
    return None


def resolve_latest_snapshot_path(
    snapshot_root: Path,
    *,
    file_name: str,
    partition_key: str = "snapshot_date",
) -> Path:
    candidates: list[tuple[str, Path]] = []
    for path in snapshot_root.glob(f"{partition_key}=*/{file_name}"):
        if not path.is_file():
            continue
        snapshot_date = _parse_date_text(extract_partition_value(path, partition_key))
        if snapshot_date:
            candidates.append((snapshot_date, path))

    if not candidates:
        raise FileNotFoundError(
            f"No {partition_key} snapshots found under {snapshot_root}"
        )

    latest_snapshot_date, latest_path = max(
        candidates,
        key=lambda item: (item[0], str(item[1])),
    )
    LOGGER.info(
        "Resolved latest Bronze snapshot: %s=%s path=%s",
        partition_key,
        latest_snapshot_date,
        latest_path,
    )
    return latest_path


def list_run_partitions(
    category_dir: Path,
    *,
    partition_key: str = "month",
) -> list[str]:
    if not category_dir.exists():
        return []
    if partition_key == "month":
        partitions: list[str] = []
        for year_dir in category_dir.glob("year=*"):
            if not year_dir.is_dir():
                continue
            year = year_dir.name.split("=", 1)[1]
            for month_dir in year_dir.glob("month=*"):
                if month_dir.is_dir():
                    month = month_dir.name.split("=", 1)[1]
                    partitions.append(f"{year}-{month}")
        return sorted(set(partitions))

    partitions = [
        p.name.split("=", 1)[1]
        for p in category_dir.iterdir()
        if p.is_dir() and p.name.startswith(f"{partition_key}=")
    ]
    return sorted(set(partitions))


def resolve_run_partitions(
    category_dir: Path,
    run_partition: str | None = None,
    *,
    partition_key: str = "month",
    min_partition_exclusive: str | None = None,
) -> list[str]:
    if run_partition:
        selected = (
            _parse_month_text(run_partition)
            if partition_key == "month"
            else str(run_partition).strip()
        )
        if not selected:
            raise ValueError(f"Invalid Bronze partition value: {run_partition}")
        path = (
            _month_dir(category_dir, selected)
            if partition_key == "month"
            else category_dir / f"{partition_key}={selected}"
        )
        if not path.is_dir():
            raise FileNotFoundError(f"Bronze partition not found: {path}")
        return [selected]

    partitions = list_run_partitions(category_dir, partition_key=partition_key)
    if not partitions:
        raise FileNotFoundError(
            f"No {partition_key} partitions found under {category_dir}"
        )

    if partition_key == "month":
        watermark_month = _parse_month_text(min_partition_exclusive)
        if watermark_month:
            partitions = [p for p in partitions if p >= watermark_month]
        return partitions

    watermark = _parse_date_text(min_partition_exclusive)
    if watermark:
        partitions = [
            p
            for p in partitions
            if (parsed := _parse_date_text(p)) is not None and parsed > watermark
        ]
    return partitions


def resolve_run_partition(category_dir: Path, run_partition: str | None = None) -> str:
    partitions = resolve_run_partitions(category_dir, run_partition)
    if not partitions:
        raise FileNotFoundError(f"No month partitions found under {category_dir}")
    return partitions[-1]


def read_partitioned_parquet(
    *,
    dataset: str,
    category_dir: Path,
    run_partition: str | None = None,
    file_glob: str = "*.parquet",
    partition_key: str = "month",
    min_partition_exclusive: str | None = None,
) -> BronzeBatch:
    selected_partitions = resolve_run_partitions(
        category_dir,
        run_partition,
        partition_key=partition_key,
        min_partition_exclusive=min_partition_exclusive,
    )
    if not selected_partitions:
        LOGGER.info(
            "No new Bronze %s partitions after watermark=%s",
            dataset,
            min_partition_exclusive,
        )
        return BronzeBatch(dataset, pd.DataFrame(), [], None, [])

    files: list[Path] = []
    for selected_partition in selected_partitions:
        partition_dir = (
            _month_dir(category_dir, selected_partition)
            if partition_key == "month"
            else category_dir / f"{partition_key}={selected_partition}"
        )
        files.extend(sorted(p for p in partition_dir.glob(file_glob) if p.is_file()))
    if not files:
        raise FileNotFoundError(f"No parquet files found in {category_dir}")

    frames: list[pd.DataFrame] = []
    for path in files:
        partition = (
            _parse_month_text(
                f"{extract_partition_value(path, 'year')}-{extract_partition_value(path, 'month')}"
            )
            if partition_key == "month"
            else extract_partition_value(path, partition_key)
        )
        df = pd.read_parquet(path)
        df = df.copy()
        df["source_file"] = str(path)
        df["run_partition"] = partition
        if partition_key == "month":
            df["bronze_month_partition"] = partition
        elif partition_key == "trading_date":
            df["bronze_trading_date_partition"] = partition
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True, sort=False)
    watermark = _parse_date_text(min_partition_exclusive)
    if watermark and partition_key == "month":
        date_col = _find_trading_date_column(combined)
        if date_col is None:
            raise ValueError("Cannot apply watermark: missing trading_date/time/date")
        trading_dates = pd.to_datetime(combined[date_col], errors="coerce")
        combined = combined.loc[trading_dates > pd.Timestamp(watermark)].copy()
        if combined.empty:
            LOGGER.info(
                "No new Bronze %s rows after trading_date watermark=%s",
                dataset,
                min_partition_exclusive,
            )
            return BronzeBatch(dataset, pd.DataFrame(), files, None, selected_partitions)

    batch_partition = selected_partitions[0] if len(selected_partitions) == 1 else None
    LOGGER.info(
        "Read Bronze %s: files=%s rows=%s partitions=%s",
        dataset,
        len(files),
        len(combined),
        len(selected_partitions),
    )
    return BronzeBatch(dataset, combined, files, batch_partition, selected_partitions)


def read_master_parquet(*, dataset: str, path: Path) -> BronzeBatch:
    if not path.is_file():
        raise FileNotFoundError(f"Bronze master parquet not found: {path}")
    df = pd.read_parquet(path).copy()
    df["source_file"] = str(path)
    LOGGER.info("Read Bronze %s master: rows=%s path=%s", dataset, len(df), path)
    return BronzeBatch(dataset, df, [path], None)


def write_single_part_parquet(
    df: pd.DataFrame,
    output_dir: Path,
    *,
    cfg: SilverConfig | None = None,
) -> Path:
    config = cfg or SilverConfig()
    output_dir.mkdir(parents=True, exist_ok=True)
    for old in output_dir.glob("PART-*.parquet"):
        try:
            old.unlink()
        except FileNotFoundError:
            pass
    output_path = output_dir / config.part_filename
    df.to_parquet(output_path, engine="pyarrow", index=False)
    LOGGER.info("Wrote Silver parquet: rows=%s path=%s", len(df), output_path)
    return output_path
