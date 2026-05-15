# Stock Pipeline Medallion Architecture

Du an: **Vietnam Stock Market Data Pipeline & Analysis Application**.

Muc tieu gan nhat la giu nguyen ingestion da chay duoc, sau do xay tiep pipeline theo Medallion Architecture:

```text
Bronze  ->  Silver  ->  Gold  ->  API  ->  Dashboard
raw files   clean DB   marts     FastAPI   React
```

RAG/chatbot la phan mo rong sau MVP, khong nam tren duong chinh hien tai.

## 1. Nguyen tac refactor

- Khong viet lai va khong di chuyen code ingestion cu trong `ingestion/`.
- Khong doi layout Bronze hien co trong `data-lake/raw/`.
- Bronze chi la file local, khong tao `raw_*` tables trong PostgreSQL.
- Silver la tang clean/typed/deduped, co ca file Parquet va PostgreSQL schema `silver`.
- Gold la tang serving/business-ready, tao bang PostgreSQL schema `gold` bang dbt.
- Backend chi doc tu Gold, frontend chi goi backend.

## 2. Current Ingestion Layer

Code ingestion hien tai van la nen tang cua project.

```text
ingestion/
├── common/                  # logging, dotenv, retry, rate limit, parquet helper
├── structure_data/           # price, index, listing, company, financial ratio, price board
├── unstructured_data/        # RSS/HTML news ingestion
└── semi_structure_data/      # HNX BCTC PDF crawl/download/metadata
```

### Structured data

Entrypoints chinh:

- `run_structure_ingestion_pipeline`
- `run_structure_full_ingestion_pipeline`
- `run_financial_ratio_ingestion_pipeline`

Bronze output:

```text
data-lake/raw/Structure_Data/
├── price/date=<run_date>/<TICKER>.parquet
├── index/date=<run_date>/<INDEX>.parquet
├── listing/master/listing.parquet
├── company/master/company_overview.parquet
├── financial_ratio/date=<run_date>/<TICKER>.parquet
└── price_board/date=<run_date>/PRICE_BOARD_SNAPSHOT.parquet
```

### Unstructured data

Entrypoint chinh:

- `ingest_news`

Bronze output:

```text
data-lake/raw/Unstructure_Data/news/
├── rss/date=<run_date>/PART-000.parquet
└── html/date=<run_date>/PART-000.parquet
```

### Semi-structured data

Entrypoint chinh:

- `run_bctc_annual_pipeline`

Bronze output:

```text
data-lake/raw/Semi_Structure_Data/
├── bctc_annual_pdf/source=hnx/date=<run_date>/ticker=<TICKER>/year=<YYYY>/<doc_id>.pdf
└── bctc_annual_pdf_meta/source=hnx/date=<run_date>/PART-000.parquet
```

## 3. Target Codebase Layout

Skeleton moi duoc them de tach ro tung tang. Cac file Python ingestion cu khong bi doi.

```text
stock-pipeline/
├── ingestion/                # Bronze ingestion, existing code
├── data-lake/
│   ├── raw/                  # Bronze, existing layout
│   └── silver/               # Silver Parquet, generated later
├── pipeline/
│   └── silver/               # Bronze -> Silver transformers/loaders
├── warehouse/
│   └── ddl/                  # PostgreSQL/TimescaleDB DDL
├── transform/
│   ├── analytics/            # pandas indicators/sentiment helpers
│   └── dbt/                  # Silver -> Gold dbt project
├── dags/                     # Airflow DAGs
├── backend/                  # FastAPI, reads Gold only
├── frontend/                 # React dashboard
├── tests/                    # unit/integration tests
└── MEDALLION_MVP_PLAN.md     # module-by-module implementation plan
```

## 4. Medallion Data Flow

```text
Sources
  ├── vnstock / KBS / VCI
  ├── RSS / HTML news
  └── HNX disclosure PDFs
        |
        v
Bronze: data-lake/raw/
  - immutable-ish source landing files
  - current ingestion modules write here
        |
        v
Silver: data-lake/silver/ + PostgreSQL silver schema
  - clean types
  - dedupe
  - validate
  - one source of truth per entity
        |
        v
Gold: PostgreSQL gold schema
  - dbt marts
  - mart_stock_daily
  - mart_company_profile
        |
        v
Serving
  - FastAPI reads Gold only
  - React/Recharts renders dashboard
```

## 5. Silver Targets

Silver tables/files to build next:

- `silver.price`
- `silver.index_price`
- `silver.listing`
- `silver.company`
- `silver.financial_ratio`
- `silver.news`
- `silver.bctc_pdf_meta`
- `silver.bctc_facts`
- `silver.price_indicator`

DDL draft is stored at:

```text
warehouse/ddl/schema.sql
```

## 6. Gold Targets

Gold tables are built from Silver by dbt:

- `gold.mart_stock_daily`
- `gold.mart_company_profile`

Planned dbt location:

```text
transform/dbt/
```

## 7. Smoke Test Existing Ingestion

### Structured smoke

Small run: one ticker, one index, no company/full financial ratio by default.

```powershell
python -c "from ingestion.common import configure_logging; configure_logging(); from ingestion.structure_data import IngestionConfig, run_structure_ingestion_pipeline; cfg=IngestionConfig(tickers=['FPT'], index_tickers=['VNINDEX'], max_tickers_per_run=1, run_partition='smoke-structure', delay_between_categories_sec=0, use_incremental_window=True, incremental_window_days=5, min_ohlcv_rows_stock_incremental=1, min_ohlcv_rows_index_incremental=1); print(run_structure_ingestion_pipeline(cfg, include_listing=False, include_company=False, include_financial_ratio=False, include_price_board=False))"
```

### News smoke

Small run with RSS only to avoid slow HTML detail crawl:

```powershell
python -c "from ingestion.common import configure_logging; configure_logging(); from ingestion.unstructured_data import NewsIngestionConfig, ingest_news; cfg=NewsIngestionConfig(enable_rss=True, enable_html=False, max_articles_per_source=5, rss_max_per_feed=5, days_back=7, days_back_rss=7, use_listing_tickers=False, tickers=['FPT','HPG','VCB'], rate_limit_rpm=120, append_only=False, truncate_partition=True); print(ingest_news(cfg))"
```

### BCTC smoke

Small HNX crawl, download disabled if you only want metadata-path validation:

```powershell
$env:HNX_CRAWL_MAX_LIST_PAGES = "1"
python -c "from ingestion.common import configure_logging; configure_logging(); from ingestion.semi_structure_data import SemiStructuredIngestionConfig, run_bctc_annual_pipeline; cfg=SemiStructuredIngestionConfig(tickers=['PVI'], run_partition='smoke-bctc-pvi'); print(run_bctc_annual_pipeline(cfg, include_download=True))"
```

## 8. Next Step

Doc ke hoach tiep theo nam trong:

```text
MEDALLION_MVP_PLAN.md
```

Doc nay chia theo module de cai tool va implement sau khi ingestion da duoc xac nhan chay on.
