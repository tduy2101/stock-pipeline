from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .bronze_reader import SilverWriteResult, write_single_part_parquet
from .config import SilverConfig
from .runs_log import write_runs_entry
from .text_utils import normalize_text, normalize_text_series, upper_nullable
from .ticker_match import (
    find_mentions_in_parts,
    load_stock_universe,
    pick_primary_ticker,
)

LOGGER = logging.getLogger(__name__)

NEWS_OUTPUT_COLUMNS = [
    "article_id",
    "source",
    "ticker",
    "ticker_mentions",
    "title",
    "summary",
    "body_text",
    "url",
    "published_at",
    "published_date",
    "fetched_at",
    "language",
    "word_count",
    "sentiment_score",
    "sentiment_label",
    "sentiment_method",
    "raw_ref",
    "run_partition",
    "source_file",
    "silver_loaded_at",
]

NEWS_INPUT_COLUMNS = [
    "article_id",
    "source",
    "ticker",
    "title",
    "summary",
    "body_text",
    "url",
    "published_at",
    "fetched_at",
    "language",
    "raw_ref",
]

POSITIVE_KEYWORDS = (
    "tăng",
    "vượt",
    "lãi",
    "lợi nhuận",
    "kỷ lục",
    "tích cực",
    "phục hồi",
    "khả quan",
)
NEGATIVE_KEYWORDS = (
    "giảm",
    "lỗ",
    "lao dốc",
    "sụt giảm",
    "áp lực",
    "cảnh báo",
    "tiêu cực",
    "thua lỗ",
)

# html=0 wins over rss=1 when sorting ascending on _source_priority
_STREAM_PRIORITY = {"html": 0, "rss": 1}

@dataclass(slots=True)
class NewsTransformResult:
    dataframe: pd.DataFrame
    input_rows: int
    input_files: list[Path] = field(default_factory=list)
    dq_warnings: list[str] = field(default_factory=list)


def _empty_news_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "article_id": pd.Series(dtype="string"),
            "source": pd.Series(dtype="string"),
            "ticker": pd.Series(dtype="string"),
            "ticker_mentions": pd.Series(dtype="object"),
            "title": pd.Series(dtype="string"),
            "summary": pd.Series(dtype="string"),
            "body_text": pd.Series(dtype="string"),
            "url": pd.Series(dtype="string"),
            "published_at": pd.Series(dtype="datetime64[ns, UTC]"),
            "published_date": pd.Series(dtype="object"),
            "fetched_at": pd.Series(dtype="datetime64[ns, UTC]"),
            "language": pd.Series(dtype="string"),
            "word_count": pd.Series(dtype="Int64"),
            "sentiment_score": pd.Series(dtype="Int64"),
            "sentiment_label": pd.Series(dtype="string"),
            "sentiment_method": pd.Series(dtype="string"),
            "raw_ref": pd.Series(dtype="string"),
            "run_partition": pd.Series(dtype="string"),
            "source_file": pd.Series(dtype="string"),
            "silver_loaded_at": pd.Series(dtype="datetime64[ns, UTC]"),
        }
    )[NEWS_OUTPUT_COLUMNS]


def _read_news_inputs(
    config: SilverConfig,
    run_partition: str,
) -> tuple[pd.DataFrame, list[Path], int]:
    frames: list[pd.DataFrame] = []
    files: list[Path] = []
    input_rows = 0
    for stream in ("html", "rss"):
        path = config.news_bronze_path(stream, run_partition)
        if not path.is_file():
            LOGGER.info("Bronze news %s missing, skip: %s", stream, path)
            continue
        df = pd.read_parquet(path).copy()
        input_rows += len(df)
        for col in NEWS_INPUT_COLUMNS:
            if col not in df.columns:
                df[col] = pd.NA
        df = df[NEWS_INPUT_COLUMNS]
        df["source_file"] = str(path)
        df["run_partition"] = run_partition
        df["_source_priority"] = _STREAM_PRIORITY[stream]
        frames.append(df)
        files.append(path)

    if not frames:
        return pd.DataFrame(), files, 0
    return pd.concat(frames, ignore_index=True, sort=False), files, input_rows


def _load_ticker_universe(config: SilverConfig) -> frozenset[str]:
    return load_stock_universe(config.listing_bronze_path())


def _combined_text(row: pd.Series) -> str:
    parts = [row.get("title"), row.get("summary"), row.get("body_text")]
    return " ".join(str(part) for part in parts if not pd.isna(part))


def _mentions_for_row(row: pd.Series, universe: frozenset[str]) -> list[str]:
    return find_mentions_in_parts(
        [row.get("title"), row.get("summary"), row.get("body_text")],
        universe,
    )


def _resolve_ticker(bronze_ticker: object, mentions: list[str]) -> object:
    if not mentions:
        return pd.NA
    if pd.isna(bronze_ticker):
        return mentions[0]
    code = str(bronze_ticker).strip().upper()
    if code in mentions:
        return code
    return pick_primary_ticker(mentions)


def _word_count(text: str) -> int:
    if not text:
        return 0
    return len(re.findall(r"\S+", text))


def _keyword_count(text: str, keywords: tuple[str, ...]) -> int:
    lowered = text.lower()
    return sum(lowered.count(keyword) for keyword in keywords)


def _sentiment(text: str) -> tuple[int, str]:
    score = _keyword_count(text, POSITIVE_KEYWORDS) - _keyword_count(
        text,
        NEGATIVE_KEYWORDS,
    )
    if score > 0:
        return score, "positive"
    if score < 0:
        return score, "negative"
    return score, "neutral"


def transform_news(
    config: SilverConfig,
    *,
    run_partition: str,
) -> NewsTransformResult:
    raw, files, input_rows = _read_news_inputs(config, run_partition)
    warnings: list[str] = []
    loaded_at = pd.Timestamp(datetime.now(timezone.utc))
    if raw.empty:
        out = _empty_news_frame()
        out["run_partition"] = out["run_partition"].astype("string")
        return NewsTransformResult(out, input_rows=0, input_files=files, dq_warnings=warnings)

    out = raw.copy()
    out["article_id"] = out["article_id"].map(normalize_text).astype("string")
    invalid_article = out["article_id"].isna() | out["article_id"].str.strip().eq("")
    if invalid_article.any():
        warnings.append(f"dropped {int(invalid_article.sum())} rows with invalid article_id")
        out = out.loc[~invalid_article].copy()

    for col in ("title", "summary", "body_text"):
        out[col] = normalize_text_series(out[col])

    for col in ("source", "url", "language", "raw_ref", "source_file", "run_partition"):
        out[col] = out[col].map(normalize_text).astype("string")

    out["published_at"] = pd.to_datetime(out["published_at"], errors="coerce", utc=True)
    out["fetched_at"] = pd.to_datetime(out["fetched_at"], errors="coerce", utc=True)
    out["ticker"] = out["ticker"].map(upper_nullable).astype("string")

    out["_body_len"] = out["body_text"].fillna("").astype(str).str.len()
    out["_has_published_at"] = out["published_at"].notna().astype(int)
    duplicate_count = int(out.duplicated(["article_id"]).sum())
    if duplicate_count:
        warnings.append(f"dropped {duplicate_count} duplicate article_id rows")
    out = out.sort_values(
        ["article_id", "_body_len", "_has_published_at", "_source_priority"],
        ascending=[True, False, False, True],
        kind="stable",
    )
    out = out.drop_duplicates(["article_id"], keep="first").copy()

    universe = _load_ticker_universe(config)
    out["ticker_mentions"] = out.apply(lambda row: _mentions_for_row(row, universe), axis=1)
    out["ticker"] = [
        _resolve_ticker(b, m)
        for b, m in zip(out["ticker"], out["ticker_mentions"], strict=True)
    ]
    out["ticker"] = out["ticker"].astype("string")

    combined = out.apply(_combined_text, axis=1)
    out["word_count"] = combined.map(_word_count).astype("Int64")
    sentiments = combined.map(_sentiment)
    out["sentiment_score"] = sentiments.map(lambda item: item[0]).astype("Int64")
    out["sentiment_label"] = sentiments.map(lambda item: item[1]).astype("string")
    out["sentiment_method"] = "keyword_v1"
    out["published_date"] = (
        out["published_at"].dt.tz_convert("Asia/Ho_Chi_Minh").dt.date
    )
    out["silver_loaded_at"] = loaded_at
    out["run_partition"] = run_partition

    out = out[NEWS_OUTPUT_COLUMNS].sort_values("article_id", kind="stable").reset_index(drop=True)
    LOGGER.info(
        "Transform news: input_rows=%s output_rows=%s warnings=%s",
        input_rows,
        len(out),
        len(warnings),
    )
    return NewsTransformResult(out, input_rows=input_rows, input_files=files, dq_warnings=warnings)


def run_news_silver(
    cfg: SilverConfig | None = None,
    *,
    run_partition: str,
    strict: bool = False,
) -> SilverWriteResult:
    from .news_validate import validate_news_silver

    config = cfg or SilverConfig()
    started_at = datetime.now(timezone.utc)
    try:
        transformed = transform_news(config, run_partition=run_partition)
        validation_issues = validate_news_silver(transformed.dataframe)
        errors = [msg for msg in validation_issues if msg.startswith("ERROR:")]
        warnings = list(transformed.dq_warnings) + [
            msg for msg in validation_issues if not msg.startswith("ERROR:")
        ]
        if errors:
            for err in errors:
                LOGGER.error("news DQ error: %s", err)
            if strict:
                raise ValueError("; ".join(errors))
        for warning in warnings:
            LOGGER.warning("news DQ warning: %s", warning)
        output_path = write_single_part_parquet(
            transformed.dataframe,
            config.silver_date_partition_dir("news", run_partition),
            cfg=config,
        )
        latest_values = transformed.dataframe["published_date"].dropna().astype(str)
        latest_key = max(latest_values) if not latest_values.empty else run_partition
        result = SilverWriteResult(
            dataset="news",
            output_path=output_path,
            input_rows=transformed.input_rows,
            output_rows=len(transformed.dataframe),
            input_files=len(transformed.input_files),
            run_partition=run_partition,
            dq_errors=errors,
            dq_warnings=warnings,
            output_paths=[output_path],
            trading_date_partitions=[],
        )
        write_runs_entry(
            dataset="news",
            silver_root=config.silver_root,
            run_partition=run_partition,
            started_at=started_at,
            input_rows=result.input_rows,
            output_rows=result.output_rows,
            output_path=config.silver_dataset_dir("news"),
            latest_key=latest_key,
            status="success",
        )
        return result
    except Exception as exc:
        write_runs_entry(
            dataset="news",
            silver_root=config.silver_root,
            run_partition=run_partition,
            started_at=started_at,
            input_rows=0,
            output_rows=0,
            output_path=config.silver_dataset_dir("news"),
            status="error",
            error=str(exc),
        )
        raise
