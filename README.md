# Stock Pipeline - Medallion Data Pipeline cho chứng khoán Việt Nam

Cập nhật: 2026-06-01

Dự án xây dựng hệ thống Data Pipeline và ứng dụng tra cứu, phân tích thị
trường chứng khoán Việt Nam đa nguồn. Kiến trúc hiện tại đi theo Medallion:
Bronze lưu dữ liệu gần nguồn, Silver chuẩn hóa thành parquet sạch,
PostgreSQL/TimescaleDB lưu warehouse, dbt build Gold marts, FastAPI phục vụ API
read-only, và React/Vite hiển thị dashboard.

```text
Bronze raw parquet/PDF
  -> Silver clean parquet
  -> PostgreSQL schema silver
  -> dbt schema gold
  -> FastAPI read-only
  -> React dashboard
```

## Trạng Thái Hiện Tại

Đã có:

- Bronze ingestion cho 3 luồng: structured data, news, và BCTC PDF metadata.
- Silver transforms cho 8 dataset:
  - `price`
  - `index_price`
  - `listing`
  - `company`
  - `financial_ratio`
  - `price_board`
  - `news`
  - `bctc_pdf_meta`
- Audit log Silver tại `data-lake/silver/<dataset>/_runs.jsonl`.
- PostgreSQL/TimescaleDB schema `silver` và `gold`.
- Warehouse loader đọc Silver parquet, chuẩn hóa dtype, validate key, upsert
idempotent vào schema `silver`, và ghi `silver.load_audit`.
- dbt project build staging, intermediate, facts, dimensions, marts trong
schema `gold`.
- FastAPI backend read-only, chỉ đọc `gold`.
- React/Vite/TypeScript frontend đọc API, có dashboard và trang chi tiết ticker.
- Pytest config chạy được từ repo root, không cần set tay `PYTHONPATH`.

Chưa nằm trong scope hiện tại:

- Airflow DAG orchestration.
- OCR/PDF table parsing hoặc extract financial facts từ PDF.
- Authentication, authorization, write endpoints.
- Streaming realtime intraday, cloud deployment, RAG/chatbot, ML sentiment nâng cao.

## Snapshot dữ liệu local

Các số liệu dưới đây được đọc từ `data-lake/silver` trong workspace local ngày
2026-06-01. Thư mục `data-lake/` bị ignore bởi git, nên row count có thể thay
đổi sau khi rerun pipeline.


| Dataset           | Số dòng Silver | Phân vùng mới nhất              | Log chạy |
| ----------------- | ----------- | ------------------------------- | -------- |
| `price`           | 62,339      | `trading_date=2026-05-18`       | có       |
| `index_price`     | 6,234       | `trading_date=2026-05-18`       | có       |
| `listing`         | 1,535       | `current`                       | có       |
| `company`         | 50          | `current`                       | có       |
| `financial_ratio` | 15,282      | `period_type=quarter/year=2026` | có       |
| `price_board`     | 50          | `trading_date=2026-05-18`       | có       |
| `news`            | 804         | `date=2026-05-19`               | có       |
| `bctc_pdf_meta`   | 1,458       | `date=2026-05-14`               | có       |


Snapshot Warehouse/Gold đã được tài liệu hóa theo cùng dữ liệu demo:

- `silver.price`: 62,339 rows
- `silver.index_price`: 6,234 rows
- `silver.listing`: 1,535 rows
- `silver.company`: 50 rows
- `silver.financial_ratio`: 15,282 rows
- `silver.price_board`: 50 rows
- `silver.news`: 804 rows
- `silver.bctc_pdf_meta`: 1,458 rows
- `gold.mart_stock_daily`: 62,339 rows
- `gold.mart_company_profile`: 50 rows
- `gold.mart_market_overview`: 1,247 rows
- `gold.mart_stock_news_daily`: 154 rows
- `gold.fact_news_article`: explode theo `(article_id, ticker)` từ `silver.news` (bài có title + ngày đăng; xem `stg_news`)
- `gold.mart_stock_news_signal`: mới 2026-06, tin theo phiên giao dịch + weighted sentiment
- `gold.mart_price_board`: mới 2026-06, bid/ask + foreign flow
- `gold.mart_financial_summary`: mới 2026-06, financial ratio pivot wide
- `gold.mart_bctc_documents`: 952 rows
- `gold.mart_ticker_directory`: union ticker từ price, news, BCTC, company

## Cấu trúc repository

```text
stock-pipeline/
|-- ingestion/                # Mã ingestion Bronze và notebook quản lý
|   |-- structure_data/        # vnstock OHLCV, listing, company, ratios, board
|   |-- unstructured_data/     # Ingest tin tức RSS/HTML
|   `-- semi_structure_data/   # Crawl/download metadata BCTC PDF từ HNX
|-- pipeline/silver/           # Bộ biến đổi Bronze -> Silver và CLI
|-- warehouse/
|   |-- ddl/schema.sql         # Schema PostgreSQL silver/gold
|   |-- loader/                # Loader upsert Silver parquet -> PostgreSQL
|   `-- scripts/               # Script thiết lập DB
|-- transform/dbt/             # Mã nguồn dbt project
|-- backend/                   # FastAPI read-only API
|-- frontend/                  # Dashboard React/Vite
|-- tests/                     # Bộ kiểm thử pytest
|-- scripts/                   # Helper validation
|-- Docs/                      # Luồng chi tiết: Structure, News, BCTC
|-- docker-compose.yml         # TimescaleDB/PostgreSQL local
|-- dbt_project.yml            # Launcher dbt tại repo root
`-- README.md                  # Tài liệu chuẩn của dự án
```

## Tài Liệu Luồng Chi Tiết

| File | Phạm vi |
|---|---|
| [Docs/Structure_data_flow.md](Docs/Structure_data_flow.md) | vnstock OHLCV, listing, company, ratios, price board → Gold/API/UI |
| [Docs/News_data_flow.md](Docs/News_data_flow.md) | RSS/HTML news → `fact_news_article`, sentiment daily |
| [Docs/BCTC_data_flow.md](Docs/BCTC_data_flow.md) | HNX BCTC PDF crawl → `mart_bctc_documents` |

## Yêu cầu trước khi chạy

- Môi trường Python 3.12 tương thích.
- Node.js cho phát triển frontend.
- Docker Desktop cho PostgreSQL/TimescaleDB local.
- Tùy chọn `.env` sao chép từ `.env.example`.

Cài đặt phụ thuộc Python:

```powershell
pip install -r requirements.txt
pip install -r backend/requirements.txt
```

Cài đặt phụ thuộc frontend:

```powershell
cd frontend
npm install
cd ..
```

URL database mặc định:

```text
postgresql://stock:stock@localhost:55432/stock_pipeline
```

Các biến môi trường quan trọng:


| Biến                            | Mục đích                                                        |
| ------------------------------ | --------------------------------------------------------------- |
| `VNSTOCK_API_KEY`              | API key vnstock (tùy chọn) cho ingestion dữ liệu cấu trúc        |
| `DATABASE_URL`                 | Kết nối PostgreSQL cho FastAPI và loader                        |
| `VITE_API_URL`                 | Base URL API cho frontend, mặc định `/api`                      |
| `HNX_SSL_VERIFY`               | Công tắc dev cho xác thực SSL HNX                               |
| `HNX_CRAWL_MAX_LIST_PAGES`     | Giới hạn số trang crawl BCTC khi test                           |
| `BCTC_INGEST_ALL_CRAWLED_PDFS` | Tải mọi PDF đã crawl thay vì chỉ báo cáo tài chính              |
| `BCTC_ALLOW_EN_DOCS`           | Cho phép BCTC tiếng Anh đi qua filter                           |
| `NEWS_RATE_LIMIT_RPM`          | Giới hạn tốc độ crawl RSS/HTML (notebook; mặc định thường 60)    |


## Runbook end-to-end

Nếu `data-lake/` đã có Bronze/Silver files, có thể bắt đầu từ layer cần chạy.
Bronze không load thẳng vào PostgreSQL; DB chỉ nhận dữ liệu từ Silver parquet.

### 1. Ingestion Bronze

Dữ liệu cấu trúc:

```powershell
@'
from ingestion.structure_data import IngestionConfig, run_structure_full_ingestion_pipeline

cfg = IngestionConfig()
print(run_structure_full_ingestion_pipeline(cfg))
'@ | python -
```

Tin tức RSS/HTML:

```powershell
@'
from ingestion.unstructured_data import NewsIngestionConfig, ingest_news

cfg = NewsIngestionConfig(days_back=30)
print(ingest_news(cfg))
'@ | python -
```

Metadata/tải PDF BCTC:

```powershell
@'
from ingestion.semi_structure_data import SemiStructuredIngestionConfig, run_bctc_annual_pipeline

cfg = SemiStructuredIngestionConfig()
print(run_bctc_annual_pipeline(cfg, include_download=True))
'@ | python -
```

Đầu ra Bronze chính:

```text
data-lake/raw/Structure_Data/price/year=<YYYY>/month=<MM>/<TICKER>.parquet
data-lake/raw/Structure_Data/index/year=<YYYY>/month=<MM>/<INDEX>.parquet
data-lake/raw/Structure_Data/listing/master/listing.parquet
data-lake/raw/Structure_Data/company/snapshots/snapshot_date=<date>/company_overview.parquet
data-lake/raw/Structure_Data/financial_ratio/snapshot_date=<timestamp>/<TICKER>.parquet
data-lake/raw/Structure_Data/price_board/snapshot_at=<timestamp>/PRICE_BOARD_SNAPSHOT.parquet
data-lake/raw/Unstructure_Data/news/<rss|html>/date=<YYYY-MM-DD>/PART-000.parquet
data-lake/raw/Semi_Structure_Data/bctc_annual_pdf_meta/source=hnx/date=<YYYY-MM-DD>/PART-000.parquet
data-lake/raw/Semi_Structure_Data/bctc_annual_pdf/source=hnx/date=<YYYY-MM-DD>/ticker=<TICKER>/year=<YYYY>/<doc_id>.pdf
```

### 2. Bronze -> Silver

Chạy tất cả dataset Silver **dữ liệu cấu trúc** (`price` … `price_board` — **không** gồm `news` / `bctc_pdf_meta`):

```powershell
python -m pipeline.silver.cli --dataset all --strict
```

Chạy từng dataset:

```powershell
python -m pipeline.silver.cli --dataset price --strict
python -m pipeline.silver.cli --dataset index_price --strict
python -m pipeline.silver.cli --dataset listing --strict
python -m pipeline.silver.cli --dataset company --strict
python -m pipeline.silver.cli --dataset financial_ratio --strict
python -m pipeline.silver.cli --dataset price_board --strict
python -m pipeline.silver.cli --dataset news --run-partition 2026-05-19 --strict
python -m pipeline.silver.cli --dataset bctc_pdf_meta --run-partition 2026-05-14 --strict
```

Đầu ra Silver:

```text
data-lake/silver/price/trading_date=<YYYY-MM-DD>/PART-000.parquet
data-lake/silver/index_price/trading_date=<YYYY-MM-DD>/PART-000.parquet
data-lake/silver/listing/current/PART-000.parquet
data-lake/silver/company/current/PART-000.parquet
data-lake/silver/financial_ratio/period_type=<quarter|annual>/year=<YYYY>/PART-000.parquet
data-lake/silver/price_board/trading_date=<YYYY-MM-DD>/PART-000.parquet
data-lake/silver/news/date=<YYYY-MM-DD>/PART-000.parquet
data-lake/silver/bctc_pdf_meta/date=<YYYY-MM-DD>/PART-000.parquet
data-lake/silver/<dataset>/_runs.jsonl
```

Quy tắc đúng của Silver:

- `price` grain: `ticker + trading_date`.
- `index_price` grain: `index_code + trading_date`.
- `listing` grain: `symbol`, stock-only universe for MVP.
- `company` grain: `ticker`, current company snapshot.
- `financial_ratio` grain: `ticker + item_code + period`, snapshot watermark dùng
full `snapshot_date` token, không cắt về date-only.
- `price_board` grain: `symbol + trading_date`, giữ latest `snapshot_at` trong ngày.
- `news` grain: `article_id`, dedupe RSS/HTML và enrich ticker/sentiment baseline.
- `bctc_pdf_meta` grain: `doc_id`, metadata/search/view PDF only.

### 3. Khởi động DB và apply DDL

Windows PowerShell:

```powershell
.\warehouse\scripts\setup_db.ps1
```

Linux/macOS/Git Bash:

```bash
bash warehouse/scripts/setup_db.sh
```

Mở psql:

```powershell
docker exec -it stock-pipeline-db psql -U stock -d stock_pipeline
```

### 4. Nạp Silver vào PostgreSQL

```powershell
$env:DATABASE_URL = "postgresql://stock:stock@localhost:55432/stock_pipeline"
python -m warehouse.loader.cli load-silver --dataset all
```

Nạp một dataset hoặc một phần:

```powershell
python -m warehouse.loader.cli load-silver --dataset price
python -m warehouse.loader.cli load-silver --dataset news
python -m warehouse.loader.cli load-silver --dataset bctc_pdf_meta
python -m warehouse.loader.cli load-silver --dataset price,index_price,news
```

Thứ tự loader hiện tại khi `all`:

```text
price -> index_price -> listing -> company -> financial_ratio -> price_board -> news -> bctc_pdf_meta
```

Bảng `silver` đang hoạt động:


| Dataset           | Table                    | Upsert key                      |
| ----------------- | ------------------------ | ------------------------------- |
| `price`           | `silver.price`           | `ticker`, `trading_date`        |
| `index_price`     | `silver.index_price`     | `index_code`, `trading_date`    |
| `listing`         | `silver.listing`         | `symbol`                        |
| `company`         | `silver.company`         | `ticker`                        |
| `financial_ratio` | `silver.financial_ratio` | `ticker`, `item_code`, `period` |
| `price_board`     | `silver.price_board`     | `symbol`, `trading_date`        |
| `news`            | `silver.news`            | `article_id`                    |
| `bctc_pdf_meta`   | `silver.bctc_pdf_meta`   | `doc_id`                        |


### 5. dbt Gold

dbt đọc schema `silver` trong PostgreSQL và build schema `gold`. Root
`dbt_project.yml` là launcher để chạy từ repo root, còn source project nằm ở
`transform/dbt/`.

```powershell
dbt debug --profiles-dir transform/dbt
dbt compile --profiles-dir transform/dbt
dbt run --profiles-dir transform/dbt
dbt test --profiles-dir transform/dbt
```

dbt sources:

```text
silver.price
silver.index_price
silver.listing
silver.company
silver.financial_ratio
silver.price_board
silver.news
silver.bctc_pdf_meta
```

Các model Gold chính:


| Layer            | Models                                                                                                                                  |
| ---------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| Staging          | `stg_price`, `stg_index_price`, `stg_listing`, `stg_company`, `stg_financial_ratio`, `stg_price_board`, `stg_news` (ephemeral), `stg_bctc_pdf_meta` (ephemeral) |
| Intermediate     | `int_price_indicator`, `int_news_sentiment_daily`                                                                                       |
| Facts/dimensions | `fact_price_daily`, `fact_index_daily`, `fact_news_article`, `dim_security`, `dim_company`                                                |
| Marts            | `mart_stock_daily`, `mart_company_profile`, `mart_market_overview`, `mart_stock_news_daily`, `mart_stock_news_signal`, `mart_price_board`, `mart_financial_summary`, `mart_bctc_documents`, `mart_ticker_directory` |


Ghi chú:

- `stg_news` và `stg_bctc_pdf_meta` là `ephemeral`; không tạo quan hệ vật lý.
- `int_price_indicator` tính daily return, MA7/20/50, RSI14, xấp xỉ MACD SMA,
Bollinger Bands, `volume_ma20` và `obv` từ `stg_price`.
- `int_news_sentiment_daily` tổng hợp sentiment keyword-v1 theo
`ticker + published_date`.

Gold models mới (2026-06):

- `mart_price_board` — bảng giá bid/ask, foreign flow
- `mart_financial_summary` — financial ratio đã pivot wide
- `mart_stock_news_signal` — tin tức theo phiên GD, weighted sentiment, `news_signal`

### 6. Backend FastAPI

Backend chỉ đọc schema `gold` và không sửa DB.

```powershell
$env:DATABASE_URL = "postgresql://stock:stock@localhost:55432/stock_pipeline"
uvicorn backend.main:app --reload --port 8000
```

Swagger UI:

```text
http://localhost:8000/docs
```

Các endpoint có sẵn:

```text
GET /health
GET /tickers
GET /market/overview
GET /market/overview?date=2026-05-18
GET /companies/{symbol}
GET /prices/{symbol}
GET /prices/{symbol}?from=2026-01-01&to=2026-05-18&page=1&page_size=100
GET /indicators/{symbol}
GET /financials/{symbol}
GET /financials/{symbol}?period_type=quarter
GET /financials/{symbol}?period_type=annual
GET /board/{symbol}
GET /board/{symbol}/foreign-flow?from=2026-05-01&to=2026-05-31
GET /news/articles?ticker=&q=&sentiment=&from=&to=&page=
GET /news/market?page_size=15
GET /news/{symbol}
GET /news/{symbol}/signal
GET /news/{symbol}/articles
GET /news/{symbol}/articles?relevance=title
GET /news/{symbol}?from=2026-05-01&to=2026-05-19
GET /bctc/documents?ticker=&year=&q=&from=&to=&page=
GET /bctc/recent?page_size=10
GET /bctc/{symbol}
GET /bctc/{symbol}?year=2025&from=2026-01-01&to=2026-05-31
GET /bctc/{symbol}/file/{doc_id}
GET /docs
```

Lineage endpoint chính (2026-06):

| Endpoint | Gold source |
|---|---|
| `GET /financials/{symbol}` | `gold.mart_financial_summary` |
| `GET /news/{symbol}` | `gold.mart_stock_news_signal` |
| `GET /news/{symbol}/signal` | `gold.mart_stock_news_signal` |
| `GET /board/{symbol}` | `gold.mart_price_board` |
| `GET /board/{symbol}/foreign-flow` | `gold.mart_price_board` |

Smoke check:

```powershell
curl http://localhost:8000/health
curl http://localhost:8000/tickers
curl http://localhost:8000/market/overview
curl http://localhost:8000/companies/VCB
curl "http://localhost:8000/prices/VCB?page_size=10"
curl http://localhost:8000/indicators/VCB
curl http://localhost:8000/financials/VCB
curl http://localhost:8000/board/VCB
curl "http://localhost:8000/board/VCB/foreign-flow?from=2026-05-01&to=2026-05-31"
curl "http://localhost:8000/news/articles?page_size=20"
curl http://localhost:8000/news/FPT
curl http://localhost:8000/news/FPT/signal
curl "http://localhost:8000/news/FPT/articles?page_size=20"
curl "http://localhost:8000/news/market?page_size=10"
curl "http://localhost:8000/bctc/documents?page_size=20"
curl "http://localhost:8000/bctc/recent?page_size=10"
curl http://localhost:8000/bctc/AAV
```

### 6.1. Current API/UI data behavior (2026-06-02)

- Dashboard index cards call `GET /market/overview?date=...` when a date is selected. If no date is selected, API returns the latest `gold.mart_market_overview.trading_date`.
- Stock detail has a shared date range filter. The range is passed to prices, indicators, board, foreign flow, stock news signal, and BCTC documents through the DB date fields (`trading_date`, mapped news `trading_date`, or BCTC `published_at::date`).
- Stock price chart renders `open`, `high`, `low`, and `close`; hover tooltip also shows `volume`.
- Company profile no longer renders `charter_capital` in the stock UI because the current dataset does not provide reliable charter capital for display.
- Financial tab reads `gold.mart_financial_summary`. `period_type=quarter` returns real quarterly rows; `period_type=annual` returns annual rows derived from the latest quarter available in each year when raw annual rows are absent.
- Stock-detail news summary reads `gold.mart_stock_news_signal`. `avg_sentiment_score` is the simple average article sentiment in the mapped trading session; `weighted_sentiment` gives larger weight to title/summary/source-tier matches; `news_signal` is positive/negative/neutral from weighted thresholds.
- Main news archive reads `gold.fact_news_article` and can preview full `body_text` only for articles where `body_text` exists.

### 7. Frontend React

Frontend là React + Vite + TypeScript, dùng TanStack Query, Axios, Recharts,
Tailwind, React Router và lucide-react. Frontend chỉ gọi FastAPI, không đọc file
và không kết nối PostgreSQL trực tiếp.

Chạy dev server:

```powershell
cd frontend
npm run dev
```

Mở:

```text
http://localhost:5173
```

Build/typecheck:

```powershell
cd frontend
npm run build
npm run typecheck
```

Các route:


| Route            | Mục đích                                                                                       |
| ---------------- | ---------------------------------------------------------------------------------------------- |
| `/`              | Market dashboard, index cards, search (`mart_ticker_directory`), top movers, tin thị trường, BCTC gần đây |
| `/stock/:symbol` | Profile, chart, board/foreign flow, indicators, financials wide format, tin theo phiên + top articles, BCTC PDF |


Base URL API mặc định là `VITE_API_URL` nếu có, nếu không thì `/api`.
