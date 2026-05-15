# Medallion Plan

File nay la roadmap sau giai doan ingestion. Muc tieu la de doc theo module, cai dung cong cu, va implement tung tang ma khong dung vao code ingestion cu.

## Module 0 - Current Bronze Ingestion

Trang thai:

- `ingestion/structure_data/` da ghi gia, index, listing, company, financial ratio, price board vao `data-lake/raw/Structure_Data/`.
- `ingestion/unstructured_data/` da ghi RSS/HTML news vao `data-lake/raw/Unstructure_Data/news/`.
- `ingestion/semi_structure_data/` da crawl/download BCTC PDF va metadata vao `data-lake/raw/Semi_Structure_Data/`.

Nguyen tac:

- Khong doi path Bronze.
- Khong di chuyen module ingestion.
- Moi refactor tiep theo doc Bronze lam input.

Can nam truoc khi di tiep:

- pandas Parquet IO
- pyarrow
- retry/rate limit
- partition file layout

## Module 1 - Silver File Layer

Muc tieu:

- Tao `data-lake/silver/`.
- Doc Bronze Parquet.
- Chuan hoa type, cot, duplicate, basic validation.
- Ghi lai Silver Parquet theo dataset.

Thu muc:

```text
pipeline/silver/
```

File se implement sau:

- `config.py`
- `bronze_reader.py`
- `price_transformer.py`
- `structure_transformer.py`
- `news_transformer.py`
- `bctc_pdf_parser.py`

Cong cu can tim hieu:

- pandas 2.x
- pyarrow
- pathlib
- typing/dataclass
- clean architecture cho ETL function

Acceptance:

- Chay transformer cho mot partition nho.
- Output co tai `data-lake/silver/<dataset>/date=<run_date>/PART-000.parquet`.
- Re-run cung input cho ket qua giong nhau.

## Module 2 - PostgreSQL/TimescaleDB Warehouse

Muc tieu:

- Tao schema `silver` va `gold`.
- Khong tao `raw_*` table.
- Load Silver Parquet vao PostgreSQL bang idempotent upsert.
- Dung TimescaleDB hypertable cho time-series price.

Thu muc:

```text
warehouse/ddl/
```

File da tao:

- `warehouse/ddl/schema.sql`

File se implement sau:

- `pipeline/silver/loader.py`
- `pipeline/silver/cli.py`

Cong cu can cai/tim hieu:

- Docker Compose
- PostgreSQL 16
- TimescaleDB
- SQLAlchemy 2.x
- psycopg2
- `INSERT ... ON CONFLICT DO UPDATE`

Acceptance:

- `docker compose up -d postgres` tao duoc database.
- `silver.price`, `silver.news`, `silver.bctc_pdf_meta` co du lieu.
- Chay loader 2 lan khong tang duplicate.

## Module 3 - Analytics Helpers

Muc tieu:

- Tinh chi bao ky thuat bang pandas.
- Tinh sentiment baseline bang keyword tieng Viet.
- Ghi output vao Silver, khong ghi thang Gold.

Thu muc:

```text
transform/analytics/
```

File se implement sau:

- `technical_indicators.py`
- `sentiment_baseline.py`

Cong cu can tim hieu:

- rolling window trong pandas
- exponential moving average
- RSI
- MACD
- Bollinger Bands
- keyword sentiment baseline

Acceptance:

- Input price cua mot ticker -> output MA7/20/50, RSI14, MACD, Bollinger.
- Input news -> output `sentiment_score`, `sentiment_label`.

## Module 4 - PDF BCTC Parser

Muc tieu:

- Parse BCTC PDF da download trong Bronze.
- Lay facts MVP: revenue, net_profit, total_assets, equity.
- Ghi vao `silver.bctc_facts`.

Thu muc:

```text
pipeline/silver/bctc_pdf_parser.py
```

Cong cu can cai/tim hieu:

- pdfplumber
- rapidfuzz
- regex
- table extraction limitations
- PDF scan vs PDF text

Acceptance:

- Chi parse metadata `qc_pass=true`, `status=downloaded`, `keep_for_parse=true`.
- Neu khong parse duoc thi log skip, khong fail ca pipeline.
- Facts co `doc_id`, `ticker`, `fiscal_year`, `metric_code`, `metric_value`, `confidence`.

## Module 5 - dbt Gold Marts

Muc tieu:

- Doc tu PostgreSQL schema `silver`.
- Build schema `gold`.
- Backend chi doc Gold.

Thu muc:

```text
transform/dbt/
```

Models can build:

- `stg_price`
- `stg_index_price`
- `stg_listing`
- `stg_company`
- `stg_financial_ratio`
- `stg_news`
- `stg_bctc_facts`
- `int_news_sentiment_daily`
- `int_company_latest_facts`
- `int_price_52w`
- `mart_stock_daily`
- `mart_company_profile`

Cong cu can cai/tim hieu:

- dbt-core 1.8
- dbt-postgres
- dbt sources
- materialized table/view
- dbt tests

Acceptance:

- `dbt run` pass.
- `dbt test` pass cho key columns.
- `gold.mart_stock_daily` va `gold.mart_company_profile` co du lieu demo.

## Module 6 - Airflow Orchestration

Muc tieu:

- Bien cac buoc ingest -> silver -> dbt thanh DAG co the demo.
- Moi DAG chay manual duoc.

Thu muc:

```text
dags/
```

DAGs:

- `dag_structure_daily.py`
- `dag_news_daily.py`
- `dag_bctc_quarterly.py`

Cong cu can cai/tim hieu:

- Apache Airflow 2.9
- LocalExecutor
- PythonOperator
- BashOperator
- Airflow env vars
- DAG schedule/manual trigger

Acceptance:

- Airflow UI mo duoc o `http://localhost:8080`.
- 3 DAG import khong loi.
- Chay manual tung DAG co log ro tung stage.

## Module 7 - FastAPI Backend

Muc tieu:

- API doc Gold only.
- Tra JSON cho frontend.

Thu muc:

```text
backend/app/
```

Endpoints:

- `GET /health`
- `GET /tickers`
- `GET /prices/{symbol}`
- `GET /financials/{symbol}`
- `GET /news/{symbol}`
- `GET /market/top`

Cong cu can cai/tim hieu:

- FastAPI 0.111
- SQLAlchemy 2.x
- Pydantic settings
- Uvicorn
- pagination/filter query params

Acceptance:

- `/health` tra OK.
- `/tickers` doc `gold.mart_company_profile`.
- `/prices/{symbol}` doc `gold.mart_stock_daily`.

## Module 8 - React Frontend

Muc tieu:

- Dashboard tra cuu ticker.
- Ticker detail co chart gia, chi bao, BCTC, news.

Thu muc:

```text
frontend/
```

Pages:

- `DashboardPage`
- `TickerDetailPage`

Cong cu can cai/tim hieu:

- Vite
- React
- TypeScript
- Recharts
- fetch API

Acceptance:

- Search ticker di den detail page.
- Price chart render duoc.
- News va financial panel doc duoc tu backend.

## Thu Tu Lam Khuyen Nghi

1. Xac nhan ingestion 3 nguon van chay.
2. Cai Docker/PostgreSQL/TimescaleDB.
3. Tao Silver transformer cho price/listing/news truoc.
4. Tao Silver loader.
5. Tao analytics indicators/sentiment.
6. Tao dbt Gold marts.
7. Tao API.
8. Tao UI.
9. Tao Airflow DAG de demo toan luong.

## Pham Vi Chua Lam Trong MVP Dau

- RAG/chatbot.
- MinIO/S3/cloud.
- Spark.
- Streaming realtime.
- OCR hang loat PDF scan.
- Authentication dashboard.
