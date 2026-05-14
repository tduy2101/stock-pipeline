# Stock Pipeline MVP - Ke hoach tiep tuc de tai

## 1. Muc tieu de tai

De tai: **Xay dung he thong Data Pipeline & Ung dung Tra cuu Phan tich Thi truong Chung khoan Viet Nam Da nguon**.

Muc tieu MVP la xay dung mot luong demo co chieu sau ky thuat:

1. Thu thap du lieu tu nhieu nguon va nhieu dang cau truc.
2. Luu toan bo du lieu goc vao **data lake**.
3. Nap du lieu tu data lake vao **data warehouse**.
4. Transform thanh cac bang phan tich va chi so ky thuat.
5. Cung cap API va dashboard tra cuu cho nguoi dung cuoi.

Trong MVP, **RAG/chatbot chi la phan mo rong**, khong nam tren duong chinh.

---

## 2. Nguyen tac kien truc quan trong

`data-lake/` la noi chua du lieu file cua du an, bao gom nhieu loai du lieu:

- Structured: gia, index, listing, company, financial ratio.
- Semi-structured: PDF BCTC, metadata PDF, text/table/fact parse tu PDF.
- Unstructured: tin tuc RSS/HTML, noi dung text.

`warehouse/` khong phai noi chua raw file. Thu muc nay chi nen chua:

- DDL tao schema PostgreSQL/TimescaleDB.
- Loader doc file tu `data-lake/raw/...` va nap vao DB.
- Script phuc vu kiem tra/truy van warehouse.

Quy uoc dung cho MVP:

```text
data-lake/
├── raw/                    # du lieu goc, giu nguyen layout hien co
│   ├── Structure_Data/
│   ├── Semi_Structure_Data/
│   └── Unstructure_Data/
└── processed/              # du lieu file da chuan hoa/tinh toan neu can chay local

warehouse/
├── ddl/                    # schema SQL
└── loaders/                # load raw parquet/PDF metadata vao PostgreSQL
```

Khong doi ten `Structure_Data`, `Semi_Structure_Data`, `Unstructure_Data` trong MVP de tranh pha code va du lieu da chay.

---

## 3. Hien trang codebase hien co

Repo chinh: `D:\WorkSpace\Đồ Án 2\stock-pipeline`

### 3.1. Ingestion common

Thu muc: `stock-pipeline/ingestion/common/`

Vai tro:

- Cau hinh logging UTF-8.
- Rate limit dung chung.
- Retry/backoff dung chung.
- Tim project root.
- Doc `.env`.
- Helper ghi Parquet.

Day la module nen giu lam nen tang cho cac pipeline ingestion.

### 3.2. Structured data ingestion

Thu muc: `stock-pipeline/ingestion/structure_data/`

Da co:

- `config.py`: `IngestionConfig` cho ticker, index, source, rate limit, incremental/backfill.
- `price_ingestor.py`: lay OHLCV co phieu.
- `index_ingestor.py`: lay OHLCV chi so.
- `stock_info_ingestor.py`: listing, company overview, financial ratio, price board.
- `pipeline.py`: orchestration tuan tu cho structured data.
- `README.md`: tai lieu module.

Output hien tai:

```text
data-lake/raw/Structure_Data/
├── price/date=<run_date>/<TICKER>.parquet
├── index/date=<run_date>/<INDEX>.parquet
├── listing/master/listing.parquet
├── company/master/company_overview.parquet
├── financial_ratio/date=<run_date>/<TICKER>.parquet
└── price_board/date=<run_date>/PRICE_BOARD_SNAPSHOT.parquet
```

### 3.3. Unstructured data ingestion

Thu muc: `stock-pipeline/ingestion/unstructured_data/`

Da co:

- `config.py`: `NewsIngestionConfig`.
- `sources.yaml`: RSS/HTML sources.
- `rss_adapter.py`: doc RSS feed.
- `html_list_adapter.py`: crawl HTML list/detail.
- `schema.py`: normalize URL, article id, ticker extraction, validate.
- `news_ingestor.py`: orchestrator.
- `_smoke_test_news.py`: chay thu nhanh.

Output hien tai:

```text
data-lake/raw/Unstructure_Data/news/
├── rss/date=<run_date>/PART-000.parquet
└── html/date=<run_date>/PART-000.parquet
```

### 3.4. Semi-structured data ingestion

Thu muc: `stock-pipeline/ingestion/semi_structure_data/`

Da co:

- `config.py`: `SemiStructuredIngestionConfig`.
- `providers/hnx_disclosure_provider.py`: crawl cong bo thong tin HNX.
- `document_classifier.py`: phan loai BCTC, ngon ngu, canonical document.
- `downloader.py`: download PDF co retry/resume/integrity check.
- `http_client.py`: session/timeout/SSL helper.
- `bctc_annual_pdf_ingestor.py`: crawl + download + metadata.
- `pipeline.py`: run download PDF + metadata.

Output hien tai:

```text
data-lake/raw/Semi_Structure_Data/
├── bctc_annual_pdf/source=hnx/date=<run_date>/ticker=<TICKER>/year=<YYYY>/<doc_id>.pdf
└── bctc_annual_pdf_meta/source=hnx/date=<run_date>/PART-000.parquet
```

### 3.5. Notebook manager

Thu muc: `stock-pipeline/ingestion/`

Dang co:

- `ingest_structure_data_manager.ipynb`
- `ingest_news.ipynb`
- `ingest_bctc_pdf_manager.ipynb`

Notebook nen duoc giu cho qua trinh thu nghiem, nhung pipeline chinh nen nam trong Python module va Airflow DAG.

---

## 4. Kien truc muc tieu MVP

```text
Nguon du lieu
  ├── vnstock / TCBS / VCI / KBS
  ├── RSS / HTML news sites
  └── HNX disclosure PDF
        │
        ▼
Ingestion layer
  ├── ingestion/structure_data
  ├── ingestion/unstructured_data
  └── ingestion/semi_structure_data
        │
        ▼
Data lake raw layer
  └── data-lake/raw/...
        │
        ▼
Warehouse loading layer
  ├── warehouse/ddl
  └── warehouse/loaders
        │
        ▼
Transform/analytics layer
  ├── transform/dbt
  └── transform/analytics
        │
        ▼
Serving layer
  ├── backend/FastAPI
  └── frontend/React
        │
        ▼
Nguoi dung demo
  ├── tra cuu ma CK
  ├── xem bieu do gia + chi so ky thuat
  ├── xem BCTC/financial metrics
  └── xem tin tuc + sentiment
```

---

## 5. Cau truc codebase de xay tiep

```text
stock-pipeline/
├── ingestion/
│   ├── common/
│   ├── structure_data/
│   ├── semi_structure_data/
│   └── unstructured_data/
│
├── data-lake/
│   ├── raw/
│   │   ├── Structure_Data/
│   │   ├── Semi_Structure_Data/
│   │   └── Unstructure_Data/
│   └── processed/
│       ├── technical_indicators/
│       ├── sentiment/
│       └── marts_snapshot/
│
├── warehouse/
│   ├── ddl/
│   │   └── schema.sql
│   ├── loaders/
│   │   ├── __init__.py
│   │   ├── config.py
│   │   ├── parquet_reader.py
│   │   ├── normalize.py
│   │   └── load_raw_to_postgres.py
│   └── README.md
│
├── transform/
│   ├── dbt/
│   │   ├── dbt_project.yml
│   │   ├── models/
│   │   │   ├── staging/
│   │   │   ├── intermediate/
│   │   │   └── marts/
│   │   └── profiles.example.yml
│   └── analytics/
│       ├── __init__.py
│       ├── technical_indicators.py
│       └── sentiment_baseline.py
│
├── dags/
│   ├── dag_structure_daily.py
│   ├── dag_news_daily.py
│   └── dag_bctc_quarterly.py
│
├── backend/
│   ├── app/
│   │   ├── main.py
│   │   ├── database.py
│   │   ├── settings.py
│   │   └── routers/
│   │       ├── tickers.py
│   │       ├── prices.py
│   │       ├── financials.py
│   │       ├── news.py
│   │       └── market.py
│   └── README.md
│
├── frontend/
│   ├── package.json
│   ├── index.html
│   └── src/
│       ├── App.tsx
│       ├── api.ts
│       ├── pages/
│       └── components/
│
├── tests/
│   ├── test_news_schema.py
│   ├── test_document_classifier.py
│   ├── test_technical_indicators.py
│   └── test_loader_normalize.py
│
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── README.md
```

---

## 6. Phan chia module chi tiet

### Module 1: Ingestion

Muc tieu: thu thap du lieu va ghi vao `data-lake/raw`.

Giu nguyen 3 ingestion group hien co:

1. `structure_data`
   - Gia OHLCV.
   - Index OHLCV.
   - Listing.
   - Company overview.
   - Financial ratio.
   - Price board.

2. `unstructured_data`
   - RSS news.
   - HTML news.
   - Dedupe article.
   - Extract ticker neu co.

3. `semi_structure_data`
   - Crawl HNX disclosure.
   - Classify PDF.
   - Download PDF.
   - Parse text/table/fact.
   - OCR optional.

Viec can lam tiep:

- Dam bao moi pipeline co `run_partition` de rerun idempotent.
- Ghi them log summary sau moi lan chay.
- Them smoke test nho cho tung ingestion group.

### Module 2: Data lake

Muc tieu: luu file du lieu theo tung stage.

Raw layer giu nguyen:

```text
data-lake/raw/Structure_Data/...
data-lake/raw/Semi_Structure_Data/...
data-lake/raw/Unstructure_Data/...
```

Processed layer nen them:

```text
data-lake/processed/technical_indicators/date=<run_date>/PART-000.parquet
data-lake/processed/sentiment/date=<run_date>/PART-000.parquet
data-lake/processed/marts_snapshot/date=<run_date>/stock_daily.parquet
```

Nguyen tac:

- Raw: khong sua, khong overwrite ngoai logic partition da co.
- Processed: co the regenerate tu raw.
- Warehouse: co the drop/reload tu data lake.

### Module 3: Warehouse

Muc tieu: dua du lieu da thu thap vao PostgreSQL/TimescaleDB.

Can them:

```text
warehouse/ddl/schema.sql
warehouse/loaders/load_raw_to_postgres.py
```

Bang raw nen co:

- `raw_price`
- `raw_index`
- `raw_listing`
- `raw_company`
- `raw_financial_ratio`
- `raw_price_board`
- `raw_news`
- `raw_bctc_pdf_meta`
- `raw_bctc_facts`

Bang dimension:

- `dim_ticker`
- `dim_date`
- `dim_source`

Bang fact/mart:

- `fact_price`
- `fact_news`
- `fact_financial_metric`
- `fact_technical_indicator`
- `mart_stock_daily`
- `mart_company_profile`

Loader can lam:

1. Tim file parquet moi nhat hoac theo `run_partition`.
2. Doc Parquet bang pandas/pyarrow.
3. Chuan hoa ten cot ve snake_case.
4. Them metadata:
   - `ingest_partition`
   - `source_file`
   - `loaded_at`
5. Upsert vao PostgreSQL.
6. Rerun cung partition khong tao duplicate.

### Module 4: Transform/dbt

Muc tieu: bien raw table thanh table phuc vu phan tich.

Models staging:

- `stg_price`
- `stg_index`
- `stg_listing`
- `stg_company`
- `stg_news`
- `stg_bctc_facts`
- `stg_financial_ratio`

Models intermediate:

- `int_price_cleaned`
- `int_technical_indicators`
- `int_news_sentiment_daily`
- `int_financial_metrics_normalized`

Models marts:

- `mart_stock_daily`
- `mart_company_profile`
- `mart_news_by_ticker`

MVP nen uu tien `mart_stock_daily` vi day la bang cap data truc tiep cho dashboard.

### Module 5: Analytics Python

Thu muc de them:

```text
transform/analytics/
├── technical_indicators.py
└── sentiment_baseline.py
```

`technical_indicators.py` tinh:

- MA7
- MA20
- MA50
- RSI14
- MACD 12/26/9
- Bollinger Bands 20 ngay

`sentiment_baseline.py` lam MVP sentiment:

- Danh sach tu tich cuc: tang, vuot, lai lon, ky luc, kha quan, phuc hoi.
- Danh sach tu tieu cuc: giam, lo, lao doc, sut giam, ap luc, canh bao.
- Score dua tren tan suat keyword trong title + summary.
- Output: `positive`, `neutral`, `negative`, `score`.

### Module 6: Airflow orchestration

Thu muc de them:

```text
dags/
├── dag_structure_daily.py
├── dag_news_daily.py
└── dag_bctc_quarterly.py
```

DAG 1: `dag_structure_daily`

```text
register_env
  -> run_structure_ingestion
  -> load_structure_raw_to_warehouse
  -> run_dbt_price_models
  -> run_technical_indicators
```

DAG 2: `dag_news_daily`

```text
run_news_ingestion
  -> load_news_raw_to_warehouse
  -> run_sentiment_baseline
  -> run_dbt_news_models
```

DAG 3: `dag_bctc_quarterly`

```text
run_bctc_download
  -> run_bctc_parse
  -> load_bctc_raw_to_warehouse
  -> run_dbt_financial_models
```

Nguyen tac DAG:

- Moi task chay duoc rieng.
- Moi task co log summary.
- Rerun khong tao duplicate.
- BCTC co the trigger manual trong demo.

### Module 7: Backend FastAPI

Thu muc de them:

```text
backend/app/
├── main.py
├── database.py
├── settings.py
└── routers/
```

Endpoint MVP:

```text
GET /health
GET /tickers
GET /tickers/{symbol}
GET /prices/{symbol}?from=YYYY-MM-DD&to=YYYY-MM-DD
GET /financials/{symbol}
GET /news/{symbol}?limit=20
GET /market/top
```

Nguon data backend:

- Uu tien doc PostgreSQL mart tables.
- Neu chua co DB trong giai doan demo som, co the fallback doc Parquet tu `data-lake/processed`.

### Module 8: Frontend React

Thu muc de them:

```text
frontend/src/
├── App.tsx
├── api.ts
├── pages/
│   ├── DashboardPage.tsx
│   └── TickerDetailPage.tsx
└── components/
    ├── TickerSearch.tsx
    ├── PriceChart.tsx
    ├── TechnicalPanel.tsx
    ├── FinancialPanel.tsx
    └── NewsPanel.tsx
```

Man hinh MVP:

1. Dashboard:
   - Search ticker.
   - Top movers.
   - Market summary.

2. Ticker detail:
   - Price chart.
   - MA/RSI/MACD summary.
   - Financial metrics.
   - Related news + sentiment badge.

Chua can lam landing page marketing. Mo app la vao dashboard tra cuu.

---

## 7. Lo trinh thuc hien tung buoc

### Buoc 1: Chot tai lieu va cau truc

Deliverable:

- README me nay.
- README trong repo `stock-pipeline/README.md` neu can.
- So do module va data flow.

Trang thai hien tai: can lam ngay.

### Buoc 2: Warehouse schema

Deliverable:

- `warehouse/ddl/schema.sql`
- Tao duoc PostgreSQL schema.

Bang can co truoc:

- `raw_price`
- `raw_listing`
- `raw_news`
- `raw_bctc_facts`
- `dim_ticker`
- `fact_price`
- `mart_stock_daily`

### Buoc 3: Loader tu data lake vao warehouse

Deliverable:

- `warehouse/loaders/load_raw_to_postgres.py`
- Load duoc structured price/listing.
- Load duoc news.
- Load duoc BCTC facts.

Uu tien load sample truoc, khong can load toan bo ngay tu dau.

### Buoc 4: Technical indicators

Deliverable:

- `transform/analytics/technical_indicators.py`
- Tinh duoc MA/RSI/MACD/Bollinger cho mot ma CK.
- Luu vao DB hoac `data-lake/processed/technical_indicators`.

### Buoc 5: Sentiment baseline

Deliverable:

- `transform/analytics/sentiment_baseline.py`
- Gan sentiment cho tin tuc.
- Output co ticker, article_id, label, score.

### Buoc 6: dbt marts

Deliverable:

- `transform/dbt/models/staging/...`
- `transform/dbt/models/marts/mart_stock_daily.sql`
- `dbt test` pass voi cac cot khoa.

Neu thieu thoi gian, co the dung SQL/Python truoc, dbt sau.

### Buoc 7: FastAPI

Deliverable:

- Backend chay local.
- Endpoint `/health` ok.
- Endpoint `/tickers`, `/prices/{symbol}`, `/news/{symbol}` tra JSON that.

### Buoc 8: React dashboard

Deliverable:

- Search ticker.
- Chart gia.
- Panel news.
- Panel financial metrics.

### Buoc 9: Airflow

Deliverable:

- 3 DAG rieng.
- Chay manual tung DAG.
- Airflow UI dung de demo data pipeline.

### Buoc 10: Test va demo script

Deliverable:

- Test unit co ban.
- Demo checklist.
- Slide luong du lieu.

---

## 8. Ke hoach 15 tuan goi y

| Tuan | Trong tam | Deliverable |
|---|---|---|
| 1 | Chot kien truc, README, ERD | README + schema draft |
| 2 | Warehouse schema + Docker PostgreSQL | `schema.sql`, DB local |
| 3 | Loader structured data | Load price/listing/company |
| 4 | Loader news + BCTC facts | Load news/BCTC vao raw tables |
| 5 | Clean staging tables | staging SQL/dbt |
| 6 | Technical indicators | MA/RSI/MACD/Bollinger |
| 7 | Financial metrics mart | `mart_company_profile` |
| 8 | News sentiment baseline | sentiment score |
| 9 | `mart_stock_daily` | bang tong hop cho dashboard |
| 10 | FastAPI backend | API endpoints MVP |
| 11 | React dashboard phase 1 | search + price chart |
| 12 | React dashboard phase 2 | financial/news tabs |
| 13 | Airflow DAGs | 3 DAG chay manual/daily |
| 14 | Test + fix bug | pytest/dbt/API smoke |
| 15 | Demo + bao cao | slide + demo live |

---

## 9. Test plan

### Unit test

- Ticker extraction trong news.
- News dedupe theo `article_id`.
- Document classification BCTC.
- Technical indicators voi input nho.
- Normalize financial_ratio column names.

### Data quality test

- `ticker` khong null trong price/news/facts neu co.
- `trading_date` khong null trong price.
- `article_id` unique trong news.
- `doc_id` khong null trong BCTC metadata/facts.
- `close`, `volume` khong am.

### Integration test

Chay luong nho:

```text
ingestion sample
  -> data-lake/raw
  -> warehouse loader
  -> transform mart
  -> FastAPI endpoint
  -> frontend chart
```

### Demo acceptance

Demo nen chung minh duoc:

1. Mo data lake thay raw files da duoc partition.
2. Mo Airflow thay DAG.
3. Mo PostgreSQL thay bang warehouse/mart.
4. Mo API docs thay endpoint.
5. Mo web app tim mot ma CK va thay chart/news/BCTC.

---

## 10. Lenh chay tham khao

### Chay structured ingestion

```powershell
python -c "from ingestion.common import configure_logging; configure_logging(); from ingestion.structure_data import IngestionConfig, run_structure_ingestion_pipeline; print(run_structure_ingestion_pipeline(IngestionConfig()))"
```

### Chay news ingestion

```powershell
python ingestion/unstructured_data/_smoke_test_news.py
```

### Chay BCTC pipeline

```powershell
python -c "from ingestion.common import configure_logging; configure_logging(); from ingestion.semi_structure_data import SemiStructuredIngestionConfig, run_bctc_annual_pipeline; print(run_bctc_annual_pipeline(SemiStructuredIngestionConfig(), include_download=True))"
```

### Chay voi crawl HNX gioi han 1 page de test

```powershell
$env:HNX_CRAWL_MAX_LIST_PAGES = "1"
python -c "from ingestion.common import configure_logging; configure_logging(); from ingestion.semi_structure_data import SemiStructuredIngestionConfig, run_bctc_annual_pipeline; print(run_bctc_annual_pipeline(SemiStructuredIngestionConfig(), include_download=True))"
```

---

## 11. Uu tien gan nhat

Thu tu nen lam ngay:

1. Tao `stock-pipeline/README.md` ban repo-level neu chua co.
2. Tao `warehouse/ddl/schema.sql`.
3. Tao `warehouse/loaders` de load Parquet tu `data-lake/raw`.
4. Tao `transform/analytics/technical_indicators.py`.
5. Tao `backend/app/main.py` voi `/health` va `/tickers`.
6. Tao frontend dashboard don gian.
7. Sau do moi them Airflow/dbt day du.

Ly do: ingestion da co, nen gia tri tiep theo nam o viec bien data raw thanh san pham co the demo.

---

## 12. Pham vi khong lam trong MVP dau tien

De tranh qua tai, cac muc sau nen de sau:

- RAG/chatbot.
- MinIO thay data lake local.
- HOSE disclosure crawler day du.
- PhoBERT sentiment production.
- OCR hang loat cho tat ca PDF.
- Streaming/realtime pipeline.
- Authentication cho dashboard.

---

## 13. Ket luan

Codebase hien tai da co nen tang ingestion tot cho 3 loai du lieu. Buoc tiep theo khong nen viet lai ingestion, ma nen xay cac tang con thieu:

```text
data-lake raw -> warehouse -> transform/mart -> API -> dashboard -> Airflow demo
```

Trong do, `data-lake/` la trung tam luu tru file raw da nguon. `warehouse/` chi la tang database/schema/loaders de truy van va phuc vu ung dung.
