# Silver Pipeline

Bronze-to-Silver processing for the local Medallion file layer. Silver reads
raw-ish Bronze files, normalizes schemas/types, performs focused DQ/dedupe, and
writes deterministic `PART-000.parquet` outputs.

## Files

- `config.py`: filesystem layout for raw and silver layers.
- `bronze_reader.py`: partition discovery, Bronze reads, lineage columns, single-part writes.
- `price_transformer.py`: stock/index OHLCV normalization.
- `structure_transformer.py`: listing and company current snapshots.
- `financial_ratio_transformer.py`: financial ratio wide-to-long normalization.
- `price_board_transformer.py`: price board snapshot normalization.
- `news_transformer.py`: news normalization, ticker enrichment, word counts, sentiment fields.
- `bctc_pdf_meta_transformer.py`: BCTC PDF metadata normalization.
- `text_utils.py`: shared text helpers for Silver transforms.
- `cli.py`: command-line entrypoint.

## Supported Outputs

Structured datasets:

- `data-lake/silver/price/trading_date=<YYYY-MM-DD>/PART-000.parquet`
- `data-lake/silver/index_price/trading_date=<YYYY-MM-DD>/PART-000.parquet`
- `data-lake/silver/listing/current/PART-000.parquet`
- `data-lake/silver/company/current/PART-000.parquet`
- `data-lake/silver/financial_ratio/period_type=<quarter|annual>/year=<YYYY>/PART-000.parquet`
- `data-lake/silver/price_board/trading_date=<YYYY-MM-DD>/PART-000.parquet`

Unstructured and semi-structured datasets:

- `data-lake/silver/news/date=<YYYY-MM-DD>/PART-000.parquet`
- `data-lake/silver/bctc_pdf_meta/date=<YYYY-MM-DD>/PART-000.parquet`

Each successful or failed Silver transform appends one JSON object to
`data-lake/silver/<dataset>/_runs.jsonl`.

## Tables not used in MVP

- `silver.price_indicator`: do not populate from Python. Indicators such as MA,
  RSI, MACD, and Bollinger are calculated in the dbt intermediate model
  `int_price_indicator`.
- `silver.bctc_facts`: future scope if OCR/PDF parsing produces numeric facts.
  It is not part of the MVP Silver pipeline.

Not currently implemented in Silver:

- PDF OCR/text/table/fact extraction
- PostgreSQL loader
- dbt Gold marts

## Run

Run all structured transforms only (`price`, `index_price`, `listing`, `company`,
`financial_ratio`, `price_board`):

```powershell
python -m pipeline.silver.cli --dataset all
```

Run one dataset:

```powershell
python -m pipeline.silver.cli --dataset price
python -m pipeline.silver.cli --dataset index
python -m pipeline.silver.cli --dataset index_price
python -m pipeline.silver.cli --dataset listing
python -m pipeline.silver.cli --dataset company
python -m pipeline.silver.cli --dataset financial_ratio
python -m pipeline.silver.cli --dataset price_board
python -m pipeline.silver.cli --dataset news --run-partition 2026-05-15
python -m pipeline.silver.cli --dataset bctc_pdf_meta --run-partition 2026-05-14
```

`--dataset index` is accepted as an alias for `index_price`.

Re-running the same transform overwrites the target Silver partition/current
file instead of appending rows. `write_single_part_parquet` removes old
`PART-*.parquet` files in the output directory before writing the new
`PART-000.parquet`.

## Structured Transforms

### Price and index_price

Bronze input:

- `data-lake/raw/Structure_Data/price/year=<YYYY>/month=<MM>/<TICKER>.parquet`
- `data-lake/raw/Structure_Data/index/year=<YYYY>/month=<MM>/<INDEX_CODE>.parquet`

Default incremental behavior:

- `price` resolves a watermark from existing Silver partitions and, when a
  database DSN is configured, Gold tables `gold.fact_price` and
  `gold.mart_stock_daily`.
- `index_price` resolves a watermark from existing Silver partitions.
- Bronze reads months greater than or equal to the watermark month, then filters
  rows with `trading_date > watermark`.

Explicit reprocessing:

- `--run-partition`, `--price-run-partition`, and `--index-run-partition` select
  one Bronze month. Accepted values include `YYYY-MM` or any parseable date in
  that month, for example `2026-05` or `2026-05-15`.
- The transform writes one Silver partition per `trading_date` found in the
  selected month.

OHLCV normalization:

- Canonicalizes date columns (`trading_date`, `tradingDate`, `time`, `date`).
- Uppercases stock tickers; for index data, renames ticker to `index_code`.
- Coerces OHLC fields to numeric and `volume` to nullable integer.
- Parses `trading_date` and `fetched_at`.
- Derives missing `value` as `close * volume`.
- Sets `value_is_derived` and `is_suspicious`.
- Flags negative OHLC values and `high < low` as suspicious.
- Drops rows with invalid key fields.
- Deduplicates by symbol/index + `trading_date`, keeping the latest row by
  `fetched_at`/`source_file`.
- Renames Bronze `ingested_at` to `bronze_ingested_at`.
- Preserves lineage columns `run_partition` and `source_file`.

Output layout:

- `data-lake/silver/price/trading_date=<YYYY-MM-DD>/PART-000.parquet`
- `data-lake/silver/index_price/trading_date=<YYYY-MM-DD>/PART-000.parquet`

### Listing

Bronze input:

- `data-lake/raw/Structure_Data/listing/master/listing.parquet`

Transform behavior:

- Compacts text columns.
- Uppercases `symbol` and `exchange`.
- Renames `type` to `security_type`.
- Keeps stock rows only when `security_type` is available; otherwise uses
  `id == 1` as the stock signal.
- Parses `crawled_at` and stores its original token as `run_partition`.
- Drops invalid symbols.
- Deduplicates by `symbol`, keeping the latest row.
- Preserves `source_file`.

Output:

- `data-lake/silver/listing/current/PART-000.parquet`

### Company

Bronze input:

- latest `data-lake/raw/Structure_Data/company/snapshots/snapshot_date=*/company_overview.parquet`

Transform behavior:

- Compacts text columns.
- Normalizes `ticker`, `symbol`, and `exchange`.
- Parses `founded_date`, `listing_date`, `as_of_date`, `snapshot_date`, and
  `fetched_at`.
- Coerces numeric company fields such as charter capital, employee count,
  listing price, listed volume, free float, and outstanding shares.
- Drops invalid tickers.
- Deduplicates by `ticker`, keeping the latest row by snapshot/fetch/source file.
- Preserves `run_partition` and `source_file`.

Output:

- `data-lake/silver/company/current/PART-000.parquet`

### Financial ratio

Bronze input:

- `data-lake/raw/Structure_Data/financial_ratio/snapshot_date=*/*.parquet`

Transform behavior:

- Reads snapshot partitions newer than the latest full `snapshot_date` token
  already present in Silver.
- Melts raw wide period columns such as `2026-Q1` and `2025-year` into one row
  per metric period.
- Parses `period`, `period_type`, `year`, and `quarter`.
- Normalizes `ticker`, `item_code`, `item_name`, `value`, `source`,
  `snapshot_date`, `fetched_at`, `run_partition`, and `source_file`.
- Deduplicates by `ticker + item_code + period`, keeping the latest snapshot.

Output:

- `data-lake/silver/financial_ratio/period_type=<quarter|annual>/year=<YYYY>/PART-000.parquet`

Watermark:

- Uses the full `snapshot_date` partition token, for example
  `2026-05-18T171513`, not only the date.

### Price board

Bronze input:

- `data-lake/raw/Structure_Data/price_board/snapshot_at=*/PRICE_BOARD_SNAPSHOT.parquet`

Transform behavior:

- Reads snapshot partitions whose `snapshot_at` date is newer than or equal to
  the latest Silver `trading_date` partition.
- Normalizes `symbol`, `exchange`, price fields, bid/ask levels, foreign fields,
  `snapshot_at`, `run_partition`, and `source_file`.
- Derives `trading_date` from `snapshot_at`.
- Adds `is_suspicious` for invalid price/volume patterns such as `close_price <= 0`,
  `high_price < low_price`, `ceiling_price < floor_price`, or negative volume.
- Deduplicates by `symbol + trading_date`, keeping the latest snapshot in the day.

Output:

- `data-lake/silver/price_board/trading_date=<YYYY-MM-DD>/PART-000.parquet`

Current grain:

- Daily latest snapshot (`symbol + trading_date`). If intraday history is needed,
  the grain should change to include `snapshot_at`.

## News

News Silver reads RSS and HTML Bronze partitions from
`data-lake/raw/Unstructure_Data/news/{rss,html}/date=<YYYY-MM-DD>/PART-000.parquet`.
Missing RSS or HTML files are skipped. If both are missing, the transform writes an
empty Silver file with the expected columns. Rows are normalized, validated on
`article_id`, deduplicated by `article_id` across RSS and HTML, enriched with
ticker mentions from `Structure_Data/listing/master/listing.parquet` when
available, word counts, and `keyword_v1` sentiment fields.

News dedupe boundary:

- Bronze deduplicates only within the same `source` using `source + article_id`.
- Silver deduplicates across RSS and HTML using `article_id`.
- When Silver sees duplicate `article_id` rows, it keeps the row with longer
  `body_text`, then `published_at`, then HTML over RSS.

Ticker rules (`pipeline/silver/ticker_match.py`):

- Universe: stock symbols from `listing.parquet` (`security_type=stock`).
- Blocklisted short symbols (e.g. `THU`, `USD`) require Vietnamese context
  patterns (`mã`, `cổ phiếu`, exchange hints) before matching.
- Bronze and Silver share the same matching logic via `resolved_ticker_universe()`.
- Silver sets `ticker` from filtered `ticker_mentions`; Bronze `ticker` is kept only
  if it appears in the filtered mention list.

Validate a partition:

```powershell
python scripts/validate_news_pipeline.py --run-partition 2026-05-19
python -m pipeline.silver.cli --dataset news --run-partition 2026-05-19 --strict
```

Reprocess multiple news days:

- The CLI accepts one news `--run-partition` per invocation; there is no range
  flag.
- Explicit list:

```powershell
foreach ($d in @("2026-05-14", "2026-05-15")) {
  python -m pipeline.silver.cli --dataset news --run-partition $d
}
```

- All Bronze news dates currently present:

```powershell
$roots = @(
  "data-lake/raw/Unstructure_Data/news/rss",
  "data-lake/raw/Unstructure_Data/news/html"
)
Get-ChildItem $roots -Directory -Filter "date=*" |
  ForEach-Object { $_.Name.Substring(5) } |
  Sort-Object -Unique |
  ForEach-Object { python -m pipeline.silver.cli --dataset news --run-partition $_ }
```

## BCTC PDF Metadata

BCTC PDF metadata Silver reads only metadata from
`data-lake/raw/Semi_Structure_Data/bctc_annual_pdf_meta/source=hnx/date=<YYYY-MM-DD>/PART-000.parquet`.
It does not read PDF bytes, OCR, parse text, parse tables, or produce facts.
Rows are validated on `doc_id`, normalized, deduplicated, assigned `period_key`,
`display_status`, and `is_available_for_web`, while preserving `pdf_path` and
`url_pdf` for downstream web usage.
