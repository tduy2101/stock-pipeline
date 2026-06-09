# Stock Pipeline - Medallion Data Pipeline cho chứng khoán Việt Nam

Cập nhật: 2026-06-03

Dự án xây dựng hệ thống Data Pipeline và ứng dụng tra cứu thị
trường chứng khoán Việt Nam đa nguồn. Kiến trúc hiện tại là **Medallion Data
Architecture** triển khai theo mô hình **batch ELT pipeline**: Bronze lưu dữ
liệu gần nguồn, Silver chuẩn hóa thành parquet sạch, PostgreSQL/TimescaleDB lưu
warehouse phân tích, dbt build Gold marts, FastAPI phục vụ API read-only, và
React/Vite hiển thị **Financial Analytics Dashboard**.

```text
Bronze raw parquet/PDF
  -> Silver clean parquet
  -> PostgreSQL schema silver
  -> dbt schema gold
  -> FastAPI read-only
  -> React dashboard
```

Thuật ngữ chuẩn khi mô tả hệ thống:

- **Medallion Data Architecture** cho luồng Bronze -> Silver -> Gold.
- **Batch ELT pipeline** vì dữ liệu được ingest/transform/load theo lịch, không streaming realtime.
- **Data Warehouse / Analytical Serving Layer** cho PostgreSQL + dbt Gold + FastAPI read-only.
- **Không gọi đây là OLTP chính**: backend không có write endpoint, không phục vụ giao dịch nghiệp vụ.

## Trạng Thái Hiện Tại

Đã có:

- Bronze ingestion cho 3 luồng: structured data, news (RSS + HTML hybrid), và BCTC PDF metadata
  (structured: listing universe ~1,500+ mã HOSE/HNX; news: VnExpress, CafeF, VnEconomy HTML; BCTC: HNX crawl).
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
- **Airflow 3.2** DAG orchestration (`docker/airflow/`) — 5 DAG bronze→silver→load→dbt; xem [README_airflow.md](docker/airflow/README_airflow.md).

Chưa nằm trong scope hiện tại:

- OCR/PDF table parsing hoặc extract financial facts từ PDF.
- Authentication, authorization, write endpoints.
- Streaming realtime intraday, cloud deployment, RAG/chatbot, ML sentiment nâng cao.

## Snapshot dữ liệu local

Các số liệu dưới đây quét từ `data-lake/` trong workspace local (**2026-06-03**).
`data-lake/` bị gitignore — số liệu thay đổi sau mỗi lần ingest. **Gold (PostgreSQL)**
chỉ khớp snapshot này sau `load-silver` + `dbt run` trên cùng bộ Silver.


### Silver — partition mới nhất (đại diện “hiện tại”)

| Dataset | Rows (partition mới nhất) | Partition mới nhất | Ghi chú |
| --- | ---: | --- | --- |
| `price` | 557 | `trading_date=2026-06-03` | Nhiều partition lịch sử (~1,250 ngày) nếu đã backfill OHLCV |
| `index_price` | 5 | `trading_date=2026-06-03` | |
| `listing` | 1,532 | `current` | Bronze listing ~3,244 dòng (mọi loại CK trên sàn) |
| `company` | 703 | `current` | Full listing universe (HOSE/HNX filter) |
| `financial_ratio` | (multi) | `period_type=*/year=*` | Tích lũy nhiều kỳ / snapshot ratio |
| `price_board` | 703 | `trading_date=2026-06-03` | Silver dedupe; Bronze luôn **1** `snapshot_at`/run, số mã phụ thuộc profile chạy |
| `news` | 924 | `date=2026-06-03` | Bronze cùng ngày: RSS ~934 + HTML ~111 |
| `bctc_pdf_meta` | 1,867 | `date=2026-06-03` | Metadata crawl HNX (mọi status) |

### Bronze — đặc thù vận hành (2026-06-03)

| Path | Ghi chú |
| --- | --- |
| `Structure_Data/price_board/snapshot_at=*` | **1 partition** / run; mặc định full listing HOSE/HNX (batched, gộp 1 snapshot) |
| `Unstructured_Data/news/{rss,html}/date=2026-06-03` | Hai kênh riêng; Silver gộp + dedupe |
| `Semi_Structure_Data/bctc_annual_pdf_meta/.../date=2026-06-03` | Partition theo **ngày chạy job**, không theo ngày công bố HNX |

### Gold (PostgreSQL) — sau `dbt run`

Marts chính: `mart_stock_daily`, `mart_company_profile`, `mart_market_overview`,
`mart_stock_news_signal`, `mart_price_board`, `mart_financial_summary`,
`fact_news_article`, `mart_bctc_documents`, `mart_ticker_directory`.
Số dòng Gold phụ thuộc lần load/dbt gần nhất — tham chiếu demo cũ (~62k price, ~952 BCTC web-ready)
trong [Docs/dbt_outputs_and_lineage.md](Docs/dbt_outputs_and_lineage.md) nếu chưa rebuild.

## Cấu trúc repository

```text
stock-pipeline/
|-- ingestion/                # Mã ingestion Bronze và notebook quản lý
|   |-- structure_data/        # vnstock OHLCV, listing, company, ratios, board
|   |-- unstructured_data/     # RSS/HTML; hybrid selectors + html_discovery CLI
|   `-- semi_structure_data/   # Crawl/download metadata BCTC PDF từ HNX
|-- pipeline/silver/           # Bộ biến đổi Bronze -> Silver và CLI
|-- warehouse/
|   |-- ddl/schema.sql         # DDL schema silver + một số legacy gold; Gold hiện tại do dbt quản lý
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
| [Docs/dbt_outputs_and_lineage.md](Docs/dbt_outputs_and_lineage.md) | Silver → Gold lineage, API/UI mapping |

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
| `hnx_max_list_pages` (config)  | Số trang HNX mặc định **10** (DAG/CLI); notebook `BCTC_RUN_PROFILE=backfill` → **500** |
| `HNX_CRAWL_MAX_LIST_PAGES`     | Env tùy chọn ghi đè `hnx_max_list_pages` khi resolve              |
| `BCTC_INGEST_ALL_CRAWLED_PDFS` | Tải mọi PDF đã crawl thay vì chỉ báo cáo tài chính              |
| `BCTC_ALLOW_EN_DOCS`           | Cho phép BCTC tiếng Anh đi qua filter                           |
| `NEWS_RATE_LIMIT_RPM`          | Giới hạn tốc độ crawl RSS/HTML (notebook; mặc định thường 60)    |

Tham số `IngestionConfig` quan trọng (structured Bronze):

| Tham số | Ý nghĩa |
|---|---|
| `use_listing_as_universe` | Dùng listing làm ticker universe tự động |
| `price_batch_size` | Số mã/batch khi fetch giá OHLCV |
| `price_board_use_listing_universe` | Mặc định **`True`** = mã từ listing; `False` = watchlist 50 mã |
| `price_board_max_tickers` | Mặc định **5000** (cap full listing) |
| `price_board_batched` | Mặc định **`True`** — batch 50 mã, gộp **1** `snapshot_at`/run |
| `financial_ratio_exchange_filter` | Chỉ fetch ratio cho HOSE/HNX mặc định |
| `delay_between_batches_sec` | Nghỉ giữa batch (giây) |


## Runbook end-to-end

Nếu `data-lake/` đã có Bronze/Silver files, có thể bắt đầu từ layer cần chạy.
Bronze không load thẳng vào PostgreSQL; DB chỉ nhận dữ liệu từ Silver parquet.

### Notebook vs DAG Airflow

DAG mirror **mặc định class/config**, không copy giá trị backfill từ notebook.
Hướng dẫn vận hành: [docker/airflow/README_airflow.md](docker/airflow/README_airflow.md).


| Luồng | Notebook | DAG / `python -m` (mặc định) |
| --- | --- | --- |
| Structured | `ingest_structure_data_manager.ipynb`: `UNIVERSE_MODE=full_listing`, `RUN_PROFILE=backfill`, `PRICE_BOARD_MODE=watchlist_50` | Airflow tách thành `structured_daily` (price, index, price_board incremental) và `structured_monthly` (listing, company, financial_ratio full HOSE/HNX) |
| News | `ingest_news.ipynb`: `days_back=30` | `NewsIngestionConfig()` — `days_back=1` |
| BCTC | `ingest_bctc_pdf_manager.ipynb`: `BCTC_RUN_PROFILE=backfill` → 500 trang | `SemiStructuredIngestionConfig()` — `hnx_max_list_pages=10` |

Chi tiết: [Structure_data_flow.md](Docs/Structure_data_flow.md) · [News_data_flow.md](Docs/News_data_flow.md) · [BCTC_data_flow.md](Docs/BCTC_data_flow.md).

### Backfill -> DAG theo dataset

| Dataset | Backfill notebook | DAG mặc định sau backfill | Bronze lưu ở đâu | Silver / warehouse |
| --- | --- | --- | --- | --- |
| `listing` | Lấy full snapshot toàn bộ listing, rồi downstream dùng filter `HOSE/HNX` để tạo universe chạy structured | `structured_monthly`: refresh full listing 1 tháng 1 lần | `data-lake/raw/Structure_Data/listing/master/listing.parquet` | `data-lake/silver/listing/current/` -> `silver.listing` -> `gold.dim_security`, `gold.mart_ticker_directory` |
| `price` | Lấy full OHLCV 5 năm cho toàn bộ ticker trong listing sau filter `HOSE/HNX` | `structured_daily`: incremental, mặc định `watchlist50`, overlap ~2 ngày; có thể đổi sang universe listing bằng `STRUCTURED_DAG_UNIVERSE=listing` | `data-lake/raw/Structure_Data/price/year=<YYYY>/month=<MM>/<TICKER>.parquet` | `data-lake/silver/price/trading_date=<YYYY-MM-DD>/` -> `silver.price` -> `gold.fact_price_daily`, `gold.mart_stock_daily` |
| `index_price` | Lấy full 5 năm cho 5 chỉ số cố định `VNINDEX`, `VN30`, `HNXINDEX`, `HNX30`, `UPCOMINDEX` | `structured_daily`: incremental theo ngày cho cùng 5 chỉ số | `data-lake/raw/Structure_Data/index/year=<YYYY>/month=<MM>/<INDEX>.parquet` | `data-lake/silver/index_price/trading_date=<YYYY-MM-DD>/` -> `silver.index_price` -> `gold.fact_index_daily`, `gold.mart_market_overview` |
| `company` | Lấy snapshot company cho toàn bộ ticker tìm được từ listing universe | `structured_monthly`: refresh full HOSE/HNX 1 tháng 1 lần | `data-lake/raw/Structure_Data/company/snapshots/snapshot_date=<run_date>/company_overview.parquet` | `data-lake/silver/company/current/` -> `silver.company` -> `gold.dim_company`, `gold.mart_company_profile` |
| `financial_ratio` | Lấy full ratio cho toàn bộ ticker HOSE/HNX từ listing universe | `structured_monthly`: refresh full HOSE/HNX 1 tháng 1 lần | `data-lake/raw/Structure_Data/financial_ratio/snapshot_date=<run_date>/<TICKER>.parquet` | `data-lake/silver/financial_ratio/period_type=<...>/year=<YYYY>/` -> `silver.financial_ratio` -> `gold.mart_financial_summary`, `gold.mart_company_profile` |
| `price_board` | Notebook mặc định chỉ lấy snapshot hiện tại cho `watchlist50` | `structured_daily`: snapshot current cho `watchlist50` mỗi ngày | `data-lake/raw/Structure_Data/price_board/snapshot_at=<timestamp>/PRICE_BOARD_SNAPSHOT.parquet` | `data-lake/silver/price_board/trading_date=<YYYY-MM-DD>/` -> `silver.price_board` -> `gold.mart_price_board` |
| `news` | Lấy bài trong 30 ngày gần nhất, nhưng ghi chung vào partition của **ngày chạy backfill** | `news_daily`: `days_back=1`, mỗi ngày một run partition mới | `data-lake/raw/Unstructured_Data/news/<rss|html>/date=<run_date>/PART-000.parquet` | `data-lake/silver/news/date=<run_date>/` -> `silver.news` -> `gold.fact_news_article`, `gold.mart_stock_news_signal` |
| `bctc_pdf_meta` + PDF | Notebook hiện tại crawl 500 trang HNX gần nhất, tải metadata + PDF của các tài liệu đạt filter | `bctc_quarterly`: quét lại top 10 trang gần nhất mỗi quý, rồi upsert theo `doc_id` | `data-lake/raw/Semi_Structure_Data/bctc_annual_pdf_meta/source=hnx/date=<run_date>/PART-000.parquet` và `data-lake/raw/Semi_Structure_Data/bctc_annual_pdf/source=hnx/date=<run_date>/.../*.pdf` | `data-lake/silver/bctc_pdf_meta/date=<run_date>/` -> `silver.bctc_pdf_meta` -> `gold.mart_bctc_documents` |

Ghi chú vận hành:

- `price` và `index_price` là hai dataset structured có incremental thật theo `trading_date` + watermark.
- `news` và `bctc_pdf_meta` partition theo **ngày chạy job**, không theo `published_at`.
- `bctc_quarterly` hiện là mô hình **rescan top 10 pages + upsert `doc_id`**, chưa phải crawl incremental theo ngày công bố HNX.

### 1. Ingestion Bronze

Notebook điều phối: `ingestion/ingest_structure_data_manager.ipynb`, `ingestion/ingest_news.ipynb`, `ingestion/ingest_bctc_pdf_manager.ipynb`.

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

# DAG quarterly (mac dinh): 10 trang HNX tu trang 1
cfg = SemiStructuredIngestionConfig()
# Backfill lan dau (hoac notebook BCTC_RUN_PROFILE=backfill):
# cfg = SemiStructuredIngestionConfig(hnx_max_list_pages=500)
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
data-lake/raw/Unstructured_Data/news/<rss|html>/date=<YYYY-MM-DD>/PART-000.parquet
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

Lineage API -> Gold marts/views (đầy đủ theo backend hiện tại):

| Endpoint | Gold source | Date/filter basis |
|---|---|---|
| `GET /health` | N/A (no DB read) | N/A |
| `GET /tickers` | `gold.mart_ticker_directory` | search text trên ticker/name flags |
| `GET /market/overview` | `gold.mart_market_overview` | `date` -> `trading_date` |
| `GET /companies/{symbol}` | `gold.mart_company_profile` | ticker exact |
| `GET /prices/{symbol}` | `gold.mart_stock_daily` | `from/to` -> `trading_date` |
| `GET /indicators/{symbol}` | `gold.mart_stock_daily` | `from/to` -> `trading_date` |
| `GET /financials/{symbol}` | `gold.mart_financial_summary` | `period_type` (`quarter` or `annual`) |
| `GET /board/{symbol}` | `gold.mart_price_board` | `from/to` -> `trading_date` |
| `GET /board/{symbol}/foreign-flow` | `gold.mart_price_board` | `from/to` -> `trading_date` (limit by `days`) |
| `GET /news/articles` | `gold.fact_news_article` | `ticker,q,sentiment,relevance,from,to` (`published_date`) |
| `GET /news/market` | `gold.fact_news_article` | latest by `published_at` |
| `GET /news/{symbol}/articles` | `gold.fact_news_article` | ticker + `relevance,from,to` (`published_date`) |
| `GET /news/{symbol}/signal` | `gold.mart_stock_news_signal` | latest `trading_date` |
| `GET /news/{symbol}` | `gold.mart_stock_news_signal` | `from/to` -> `trading_date` |
| `GET /bctc/documents` | `gold.mart_bctc_documents` | `ticker,year,q,from,to` (`published_at::date`) |
| `GET /bctc/recent` | `gold.mart_bctc_documents` | latest by `published_at` |
| `GET /bctc/{symbol}` | `gold.mart_bctc_documents` | ticker + `year,from,to` (`published_at::date`) |
| `GET /bctc/{symbol}/file/{doc_id}` | `gold.mart_bctc_documents` (`pdf_path`) | doc_id lookup |

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

### 6.1. Current API/UI data behavior (2026-06-03)

- Dashboard index cards call `GET /market/overview?date=...` when a date is selected. If no date is selected, API returns the latest `gold.mart_market_overview.trading_date`.
- Stock detail has a shared date range filter. The range is passed to prices, indicators, board, foreign flow, stock news signal, and BCTC documents through the DB date fields (`trading_date`, mapped news `trading_date`, or BCTC `published_at::date`).
- Stock price chart renders `open`, `high`, `low`, and `close`; hover tooltip also shows `volume`.
- Company profile no longer renders `charter_capital` in the stock UI because the current dataset does not provide reliable charter capital for display.
- Financial tab reads `gold.mart_financial_summary`. `period_type=quarter` returns real quarterly rows; `period_type=annual` returns annual rows derived from the latest quarter available in each year when raw annual rows are absent.
- Stock-detail news summary reads `gold.mart_stock_news_signal`. `avg_sentiment_score` is the simple average article sentiment in the mapped trading session; `weighted_sentiment` gives larger weight to title/summary/source-tier matches; `news_signal` is `buy_signal`/`sell_signal`/`neutral` from weighted thresholds; `sentiment_label` and `dominant_sentiment` use `positive`/`negative`/`neutral`.
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
