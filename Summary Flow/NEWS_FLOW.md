# NEWS FLOW - Bronze to Silver to Gold

Cap nhat den ngay 2026-05-25.

Pham vi: news RSS/HTML, Bronze parquet, Silver normalize/dedupe/enrich, load vao
PostgreSQL `silver.news`, va dbt aggregate sentiment vao Gold.

## 1. Tong quan luong

```text
RSS/HTML sources
  -> ingestion/unstructured_data/*
  -> data-lake/raw/Unstructure_Data/news/*
  -> pipeline/silver/news_transformer.py
  -> data-lake/silver/news/*
  -> warehouse.loader.cli
  -> silver.news
  -> dbt stg_news/int_news_sentiment_daily
  -> gold.mart_stock_news_daily
  -> gold.mart_stock_daily
```

## 2. Code chinh

| Tang | File/module | Vai tro |
|---|---|---|
| Config | `ingestion/unstructured_data/config.py` | Source config, days_back, rate limit |
| RSS | `ingestion/unstructured_data/rss_adapter.py` | Fetch/parse RSS |
| HTML | `ingestion/unstructured_data/html_list_adapter.py` | Fetch/parse HTML list/detail |
| Ingest | `ingestion/unstructured_data/news_ingestor.py` | Orchestrate, dedupe, write Bronze |
| Schema | `ingestion/unstructured_data/schema.py` | Article id, normalize URL, validate |
| Ticker match | `pipeline/silver/ticker_match.py` | Match ticker from listing universe |
| Silver | `pipeline/silver/news_transformer.py` | Dedupe cross-source, enrich sentiment |
| Validate | `scripts/validate_news_pipeline.py` | Read-only validation/report |
| Loader | `warehouse/loader/silver_loader.py` | Load `silver.news` |
| dbt | `transform/dbt/models/` | `stg_news`, sentiment aggregate, marts |

## 3. Bronze ingest

```powershell
@'
from ingestion.unstructured_data import NewsIngestionConfig, ingest_news

cfg = NewsIngestionConfig(days_back=30)
print(ingest_news(cfg))
'@ | python -
```

Bronze outputs:

```text
data-lake/raw/Unstructure_Data/news/rss/date=<YYYY-MM-DD>/PART-000.parquet
data-lake/raw/Unstructure_Data/news/html/date=<YYYY-MM-DD>/PART-000.parquet
```

Current audited Bronze partition:

```text
date=2026-05-19
RSS rows: 820
HTML rows: 82
Combined unique article_id: 804
```

## 4. Silver news

Run:

```powershell
python -m pipeline.silver.cli --dataset news --run-partition 2026-05-19 --strict
```

Silver output:

```text
data-lake/silver/news/date=2026-05-19/PART-000.parquet
data-lake/silver/news/_runs.jsonl
```

Silver behavior:

- Reads RSS and HTML Bronze partitions for the run date.
- Dedupe across RSS/HTML by `article_id`.
- Keeps richer row when duplicated: longer `body_text`, later timestamps, HTML
  preference where applicable.
- Matches ticker using listing universe and blocklist/context rules.
- Adds `word_count`, `sentiment_score`, `sentiment_label`,
  `sentiment_method='keyword_v1'`.
- Preserves lineage columns.

Validate:

```powershell
python scripts/validate_news_pipeline.py --run-partition 2026-05-19
```

Current Silver:

| Metric | Value |
|---|---:|
| `silver.news` rows | 804 |
| unique `article_id` | 804 |
| rows with ticker | 173 |
| distinct `published_date` after staging filter | 21 |

## 5. Load news vao PostgreSQL

```powershell
$env:DATABASE_URL = "postgresql://stock:stock@localhost:55432/stock_pipeline"
python -m warehouse.loader.cli load-silver --dataset news
```

`silver.news` grain:

```text
article_id
```

Loader handles:

- `ticker_mentions` -> PostgreSQL `text[]`
- `raw_ref` -> PostgreSQL `jsonb`
- `published_date` and `run_partition` -> date
- `published_at`, `fetched_at`, `silver_loaded_at` -> timestamptz
- unique non-null URL validation
- idempotent upsert by `article_id`

## 6. dbt Gold news

`stg_news` is materialized as `ephemeral`, so it does not create physical table
`gold.stg_news`. It is inlined into downstream models.

Staging filter:

```sql
WHERE published_date IS NOT NULL
  AND ticker IS NOT NULL
  AND article_id IS NOT NULL
```

Current filtered staging rows:

```text
stg_news filtered rows: 173
distinct published_date: 21
```

Gold news models:

| Model | Grain | Rows hien tai | Noi dung |
|---|---|---:|---|
| `gold.int_news_sentiment_daily` | `ticker + published_date` | 154 | news_count, avg sentiment, positive/negative/neutral counts, dominant sentiment |
| `gold.mart_stock_news_daily` | `ticker + published_date` | 154 | rounded sentiment mart |
| `gold.mart_stock_daily` | `ticker + trading_date` | 62,339 | left join news sentiment by ticker/date |

`mart_stock_daily` join rule:

```sql
fpd.ticker = ns.ticker
AND fpd.trading_date = ns.published_date
```

Rows without news get `news_count = 0`.

## 7. End-to-end commands

```powershell
# 1. Bronze ingest
@'
from ingestion.unstructured_data import NewsIngestionConfig, ingest_news

cfg = NewsIngestionConfig(days_back=30)
print(ingest_news(cfg))
'@ | python -

# 2. Bronze -> Silver
python -m pipeline.silver.cli --dataset news --run-partition 2026-05-19 --strict

# 3. Validate Silver
python scripts/validate_news_pipeline.py --run-partition 2026-05-19

# 4. Load DB
$env:DATABASE_URL = "postgresql://stock:stock@localhost:55432/stock_pipeline"
python -m warehouse.loader.cli load-silver --dataset news

# 5. Build Gold.
# Tu DB sach nen chay full dbt run de dam bao cac dependency structured/fact da co.
dbt run --profiles-dir transform/dbt
dbt test --profiles-dir transform/dbt

# Neu cac dependency da build san, co the chay scope news:
dbt run --profiles-dir transform/dbt --select int_news_sentiment_daily mart_stock_news_daily mart_stock_daily
dbt test --profiles-dir transform/dbt --select int_news_sentiment_daily mart_stock_news_daily mart_stock_daily
```

## 8. Verify nhanh

```sql
SELECT COUNT(*) FROM silver.news;
SELECT COUNT(*) FROM gold.int_news_sentiment_daily;
SELECT COUNT(*) FROM gold.mart_stock_news_daily;

SELECT ticker, trading_date, news_count, dominant_sentiment
FROM gold.mart_stock_daily
WHERE news_count > 0
ORDER BY trading_date DESC, ticker
LIMIT 5;
```

Current sample:

```text
FPT 2026-05-18 news_count=1 dominant_sentiment=neutral
HPG 2026-05-18 news_count=1 dominant_sentiment=neutral
MSN 2026-05-18 news_count=1 dominant_sentiment=positive
NVL 2026-05-18 news_count=1 dominant_sentiment=positive
PNJ 2026-05-18 news_count=2 dominant_sentiment=neutral
```

## 9. Ghi chu van hanh

- News ingestion khong co watermark rieng; moi run la rolling window theo
  `days_back`, partition theo ngay chay.
- Silver CLI can `--run-partition` cho news.
- Rerun Silver news overwrite `PART-000.parquet` cua ngay do.
- Rerun loader upsert by `article_id`, khong tao duplicate.
- News sentiment hien la keyword method co ban, phu hop MVP; co the thay bang
  model NLP rieng sau.
