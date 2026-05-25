# Kiến trúc Medallion của Stock Pipeline

Dự án: **Ứng dụng pipeline dữ liệu và phân tích thị trường chứng khoán Việt Nam**.

Trạng thái hiện tại: cập nhật đến **2026-05-25**. Pipeline đã được triển khai qua
Bronze, Silver, PostgreSQL `silver`, và các mart Gold của dbt. FastAPI, Airflow và
React vẫn là phần việc tương lai.

```text
Tệp Bronze -> Tệp Silver -> PostgreSQL silver -> dbt gold -> API/dashboard
parquet thô      parquet sạch     bảng kho dữ liệu      marts      tương lai
```

Ghi chú luồng chi tiết nằm trong `Summary Flow/`:

- `Summary Flow/STRUCTURED_DATA_FLOW_README.md`
- `Summary Flow/NEWS_FLOW.md`
- `Summary Flow/BCTC_PDF_FLOW.md`
- `Summary Flow/GOLD_DBT_FLOW.md`

## Trạng thái hiện tại

Đã triển khai:

- Nạp Bronze cho dữ liệu có cấu trúc, tin tức và metadata PDF BCTC.
- Biến đổi Silver cho 8 bộ dữ liệu:
  - `price`
  - `index_price`
  - `listing`
  - `company`
  - `financial_ratio`
  - `price_board`
  - `news`
  - `bctc_pdf_meta`
- Nhật ký audit biến đổi Silver tại `data-lake/silver/<dataset>/_runs.jsonl`.
- Schema `silver` trên PostgreSQL/TimescaleDB với loader upsert idempotent.
- Dự án dbt đọc `silver.*` và xây dựng schema `gold`.
- Các mô hình Gold của dbt cho chỉ báo giá, cảm xúc tin tức, hồ sơ công ty,
  tổng quan thị trường, cổ phiếu theo ngày, tin tức cổ phiếu theo ngày, và tìm kiếm
  tài liệu BCTC.
- Pytest có thể chạy từ thư mục gốc repo mà không cần đặt `PYTHONPATH` thủ công.

Chưa triển khai:

- Các endpoint backend FastAPI.
- Bảng điều khiển React.
- Điều phối DAG bằng Airflow.
- OCR/bóc tách bảng PDF thành dữ liệu tài chính.

## Bố cục kho mã

```text
stock-pipeline/
|-- ingestion/                # Mã nạp Bronze và notebook
|-- data-lake/
|   |-- raw/                  # Tệp Bronze parquet/PDF
|   `-- silver/               # Tệp Silver parquet và log _runs.jsonl
|-- pipeline/silver/          # Bộ biến đổi Bronze -> Silver
|-- warehouse/
|   |-- ddl/schema.sql        # Schema PostgreSQL silver/gold
|   |-- loader/               # Loader Silver parquet -> PostgreSQL
|   `-- scripts/              # Script thiết lập DB
|-- transform/dbt/            # Nguồn dự án dbt
|-- dbt_project.yml           # Trình khởi chạy dbt ở thư mục gốc
|-- Summary Flow/             # Runbook theo luồng và ghi chú audit
|-- tests/                    # Bộ kiểm thử pytest
|-- backend/                  # FastAPI (tương lai)
|-- frontend/                 # React (tương lai)
`-- dags/                     # Airflow (tương lai)
```

## Yêu cầu trước khi chạy

- Môi trường Python với các phụ thuộc của dự án:

```powershell
pip install -r requirements.txt
```

- Docker Desktop đang chạy.
- PostgreSQL/TimescaleDB được khởi chạy qua `docker-compose.yml` trên cổng host
  `55432`.
- URL cơ sở dữ liệu mặc định:

```text
postgresql://stock:stock@localhost:55432/stock_pipeline
```

Các giá trị `.env` tùy chọn sẽ được đọc khi phù hợp. Loader mặc định dùng URL DB
ở trên nếu `DATABASE_URL` chưa được đặt.

## Thứ tự chạy end-to-end

Nếu data-lake đã có tệp Bronze/Silver, bạn có thể bỏ qua các bước đầu và bắt đầu
từ lớp cần dùng.

```text
1. Nạp Bronze          -> ghi vào data-lake/raw/*
2. Biến đổi Silver     -> ghi vào data-lake/silver/*
3. Thiết lập DB        -> tạo schema/bảng PostgreSQL
4. Nạp Silver vào DB   -> upsert parquet vào schema silver
5. Build dbt Gold      -> xây dựng schema gold từ schema silver
6. Kiểm thử/xác minh   -> test pytest/dbt/đếm SQL
```

## 1. Nạp Bronze

Bronze chỉ dựa trên file. Dữ liệu Bronze không được nạp trực tiếp vào PostgreSQL.

### Dữ liệu có cấu trúc

Các hàm Python chính:

- `run_structure_ingestion_pipeline`
- `run_structure_full_ingestion_pipeline`
- `run_financial_ratio_ingestion_pipeline`

Ví dụ:

```powershell
@'
from ingestion.structure_data import IngestionConfig, run_structure_full_ingestion_pipeline

cfg = IngestionConfig()
print(run_structure_full_ingestion_pipeline(cfg))
'@ | python -
```

Đầu ra Bronze điển hình:

```text
data-lake/raw/Structure_Data/price/year=<YYYY>/month=<MM>/<TICKER>.parquet
data-lake/raw/Structure_Data/index/year=<YYYY>/month=<MM>/<INDEX>.parquet
data-lake/raw/Structure_Data/listing/master/listing.parquet
data-lake/raw/Structure_Data/company/snapshots/snapshot_date=<date>/*.parquet
data-lake/raw/Structure_Data/financial_ratio/snapshot_date=<timestamp>/*.parquet
data-lake/raw/Structure_Data/price_board/snapshot_at=<timestamp>/PRICE_BOARD_SNAPSHOT.parquet
```

### Tin tức

Quy trình nạp tin tức đọc nguồn RSS/HTML và ghi các phân vùng Bronze theo ngày chạy.

```powershell
@'
from ingestion.unstructured_data import NewsIngestionConfig, ingest_news

cfg = NewsIngestionConfig(days_back=30)
print(ingest_news(cfg))
'@ | python -
```

Đầu ra:

```text
data-lake/raw/Unstructure_Data/news/rss/date=<YYYY-MM-DD>/PART-000.parquet
data-lake/raw/Unstructure_Data/news/html/date=<YYYY-MM-DD>/PART-000.parquet
```

### Siêu dữ liệu PDF BCTC

Hiện tại BCTC chỉ crawl/tải các tệp PDF và ghi metadata. Chưa OCR hay bóc tách bảng
từ nội dung PDF.

```powershell
@'
from ingestion.semi_structure_data import SemiStructuredIngestionConfig, run_bctc_annual_pipeline

cfg = SemiStructuredIngestionConfig()
print(run_bctc_annual_pipeline(cfg, include_download=True))
'@ | python -
```

Đầu ra:

```text
data-lake/raw/Semi_Structure_Data/bctc_annual_pdf/source=hnx/date=<YYYY-MM-DD>/ticker=<TICKER>/year=<YYYY>/<doc_id>.pdf
data-lake/raw/Semi_Structure_Data/bctc_annual_pdf_meta/source=hnx/date=<YYYY-MM-DD>/PART-000.parquet
```

## 2. Biến đổi Silver

Silver chuẩn hóa schema/kiểu dữ liệu, khử trùng lặp, đảm bảo mức hạt hiện tại, và
ghi ra các file `PART-000.parquet` có tính xác định. Chạy lại sẽ ghi đè phân vùng/
thư mục hiện tại của Silver thay vì append file trùng.

Chạy tất cả bộ dữ liệu Silver có cấu trúc:

```powershell
python -m pipeline.silver.cli --dataset all
```

Chạy một bộ dữ liệu có cấu trúc:

```powershell
python -m pipeline.silver.cli --dataset price --strict
python -m pipeline.silver.cli --dataset index_price --strict
python -m pipeline.silver.cli --dataset listing --strict
python -m pipeline.silver.cli --dataset company --strict
python -m pipeline.silver.cli --dataset financial_ratio --strict
python -m pipeline.silver.cli --dataset price_board --strict
```

Chạy tin tức và metadata BCTC cho một phân vùng Bronze đã biết:

```powershell
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
```

Mỗi lần chạy Silver sẽ ghi thêm một dòng audit:

```text
data-lake/silver/<dataset>/_runs.jsonl
```

## 3. Khởi động DB và áp dụng DDL

Windows PowerShell:

```powershell
.\warehouse\scripts\setup_db.ps1
```

Linux/macOS/Git Bash:

```bash
bash warehouse/scripts/setup_db.sh
```

Script thiết lập:

- Khởi động `stock-pipeline-db` từ `docker-compose.yml`.
- Chờ PostgreSQL sẵn sàng.
- Áp dụng `warehouse/ddl/schema.sql`.
- Tạo schema `silver` và `gold`.
- Tạo các bảng Silver đang dùng và `silver.load_audit`.

Kết nối bằng:

```text
Host: localhost
Port: 55432
Database: stock_pipeline
Username: stock
Password: stock
```

Mở psql:

```powershell
docker exec -it stock-pipeline-db psql -U stock -d stock_pipeline
```

Để reset hoàn toàn volume DB:

```powershell
docker compose down -v
.\warehouse\scripts\setup_db.ps1
```

## 4. Nạp Silver vào PostgreSQL

Loader đọc các file parquet Silver, chuẩn bị giá trị tương thích DB, kiểm tra
khóa, và upsert các dòng vào schema `silver`. Nó không nạp file Bronze.

Đặt DB URL thủ công nếu muốn:

```powershell
$env:DATABASE_URL = "postgresql://stock:stock@localhost:55432/stock_pipeline"
```

Nạp tất cả bộ dữ liệu Silver được hỗ trợ:

```powershell
python -m warehouse.loader.cli load-silver --dataset all
```

Nạp một bộ dữ liệu:

```powershell
python -m warehouse.loader.cli load-silver --dataset price
python -m warehouse.loader.cli load-silver --dataset news
python -m warehouse.loader.cli load-silver --dataset bctc_pdf_meta
```

Nạp một tập con, phân tách bằng dấu phẩy:

```powershell
python -m warehouse.loader.cli load-silver --dataset price,index_price,news
```

Thứ tự loader hiện tại cho `all`:

```text
price -> index_price -> listing -> company -> financial_ratio -> price_board -> news -> bctc_pdf_meta
```

Các bảng Silver hiện tại và khóa upsert:

| Bộ dữ liệu | Bảng DB | Khóa upsert | Số dòng hiện tại |
|---|---|---|---:|
| `price` | `silver.price` | `ticker`, `trading_date` | 62,339 |
| `index_price` | `silver.index_price` | `index_code`, `trading_date` | 6,234 |
| `listing` | `silver.listing` | `symbol` | 1,535 |
| `company` | `silver.company` | `ticker` | 50 |
| `financial_ratio` | `silver.financial_ratio` | `ticker`, `item_code`, `period` | 15,282 |
| `price_board` | `silver.price_board` | `symbol`, `trading_date` | 50 |
| `news` | `silver.news` | `article_id` | 804 |
| `bctc_pdf_meta` | `silver.bctc_pdf_meta` | `doc_id` | 1,458 |

Audit loader:

```sql
SELECT dataset, run_partition, rows_read, rows_inserted, rows_updated, status, loaded_at
FROM silver.load_audit
ORDER BY loaded_at DESC
LIMIT 30;
```

## 5. dbt Gold

dbt đọc schema PostgreSQL `silver` và xây dựng các mô hình phân tích trong schema
`gold`. File gốc `dbt_project.yml` là một launcher để có thể chạy lệnh từ thư mục
gốc repo trong khi mã nguồn dự án nằm dưới `transform/dbt/`.

Từ thư mục gốc repo:

```powershell
dbt debug --profiles-dir transform/dbt
dbt compile --profiles-dir transform/dbt
dbt run --profiles-dir transform/dbt
dbt test --profiles-dir transform/dbt
```

Từ bên trong `transform/dbt`:

```powershell
cd transform/dbt
dbt run --profiles-dir .
dbt test --profiles-dir .
cd ../..
```

Các tầng mô hình dbt:

```text
silver sources
  -> mô hình staging
  -> mô hình trung gian
  -> marts/facts/dimensions
```

### Staging

Các mô hình staging có cấu trúc được materialize thành view:

- `gold.stg_price`
- `gold.stg_index_price`
- `gold.stg_listing`
- `gold.stg_company`
- `gold.stg_financial_ratio`
- `gold.stg_price_board`

Các mô hình staging cho tin tức và BCTC là `ephemeral`, nên chúng được inline vào
SQL downstream và không tạo quan hệ vật lý `gold.stg_news` hoặc
`gold.stg_bctc_pdf_meta`:

- `stg_news`: lọc các dòng có `article_id`, `ticker`, và `published_date`.
- `stg_bctc_pdf_meta`: lọc các tài liệu có thể hiển thị trên web khi `display_status != 'error'`.

### Trung gian

- `gold.int_price_indicator`: các chỉ báo kỹ thuật từ `stg_price`.
  - Lợi suất ngày
  - MA7, MA20, MA50
  - RSI14 theo phương pháp Cutler/SMA
  - MACD xấp xỉ dựa trên SMA
  - Dải Bollinger
- `gold.int_news_sentiment_daily`: tổng hợp cảm xúc tin tức theo ticker/ngày.

### Gold Marts

| Mô hình | Mức hạt | Vai trò | Số dòng hiện tại |
|---|---|---|---:|
| `gold.dim_security` | `symbol` | Dimension chứng khoán niêm yết | 1,535 |
| `gold.dim_company` | `ticker` | Dimension hồ sơ công ty | 50 |
| `gold.fact_price_daily` | `ticker`, `trading_date` | Fact giá kèm chỉ báo | 62,339 |
| `gold.fact_index_daily` | `index_code`, `trading_date` | Fact chỉ số theo ngày | 6,234 |
| `gold.mart_stock_daily` | `ticker`, `trading_date` | Mart cổ phiếu theo ngày với chỉ báo và cảm xúc tin tức | 62,339 |
| `gold.mart_company_profile` | `ticker` | Hồ sơ công ty, giá mới nhất, thống kê 52w, khối lượng 20d, PE/PB/EPS/ROE/ROA | 50 |
| `gold.mart_market_overview` | `trading_date` | Tổng quan thị trường, pivot chỉ số, top tăng/giảm JSON | 1,247 |
| `gold.mart_stock_news_daily` | `ticker`, `published_date` | Mart cảm xúc tin tức theo ngày | 154 |
| `gold.mart_bctc_documents` | `doc_id` | Mart tìm kiếm/xem tài liệu BCTC sẵn có | 952 |

Số lượng test dbt hiện tại:

```text
dbt test --profiles-dir transform/dbt
PASS=48 WARN=0 ERROR=0
```

## dbt hoạt động trong repo này như thế nào

- `transform/dbt/profiles.yml` trỏ dbt tới PostgreSQL trên cổng `55432`.
- `transform/dbt/models/staging/sources.yml` khai báo schema nguồn
  `silver`.
- `{{ source('silver', '<table>') }}` đọc các bảng warehouse được nạp bởi
  `warehouse.loader.cli`.
- `{{ ref('<model>') }}` tạo DAG giữa các mô hình dbt.
- `dbt run` build mô hình theo thứ tự phụ thuộc.
- `dbt test` chạy schema test từ `models/**/schema.yml` và test generic tùy chỉnh
  `unique_combination_of_columns`.
- dbt không upload file parquet. Bước upload/upsert thuộc về warehouse loader.
  dbt chỉ biến đổi các bảng đã có trong PostgreSQL.

## Xác minh trạng thái hiện tại

Đếm Silver:

```sql
SELECT 'price' AS table_name, COUNT(*) FROM silver.price
UNION ALL SELECT 'index_price', COUNT(*) FROM silver.index_price
UNION ALL SELECT 'listing', COUNT(*) FROM silver.listing
UNION ALL SELECT 'company', COUNT(*) FROM silver.company
UNION ALL SELECT 'financial_ratio', COUNT(*) FROM silver.financial_ratio
UNION ALL SELECT 'price_board', COUNT(*) FROM silver.price_board
UNION ALL SELECT 'news', COUNT(*) FROM silver.news
UNION ALL SELECT 'bctc_pdf_meta', COUNT(*) FROM silver.bctc_pdf_meta;
```

Đếm Gold:

```sql
SELECT 'fact_price_daily' AS model, COUNT(*) FROM gold.fact_price_daily
UNION ALL SELECT 'fact_index_daily', COUNT(*) FROM gold.fact_index_daily
UNION ALL SELECT 'mart_stock_daily', COUNT(*) FROM gold.mart_stock_daily
UNION ALL SELECT 'mart_company_profile', COUNT(*) FROM gold.mart_company_profile
UNION ALL SELECT 'mart_market_overview', COUNT(*) FROM gold.mart_market_overview
UNION ALL SELECT 'mart_stock_news_daily', COUNT(*) FROM gold.mart_stock_news_daily
UNION ALL SELECT 'mart_bctc_documents', COUNT(*) FROM gold.mart_bctc_documents;
```

Các chỉ số công ty được lấy từ `silver.financial_ratio`:

```sql
SELECT
  COUNT(*) AS total,
  COUNT(pe_ratio) AS has_pe,
  COUNT(pb_ratio) AS has_pb,
  COUNT(eps) AS has_eps,
  COUNT(roe) AS has_roe,
  COUNT(roa) AS has_roa
FROM gold.mart_company_profile;
```

Tin tức đã được join vào `mart_stock_daily`:

```sql
SELECT ticker, trading_date, news_count, dominant_sentiment
FROM gold.mart_stock_daily
WHERE news_count > 0
ORDER BY trading_date DESC, ticker
LIMIT 5;
```

## Kiểm thử

Chạy test Python:

```powershell
python -m pytest -q
```

Chạy test dbt:

```powershell
dbt test --profiles-dir transform/dbt
```

## Bảng đã ngừng dùng / phạm vi tương lai

Pipeline hiện tại không nạp các bảng sau:

- `silver.price_indicator`: các chỉ báo kỹ thuật hiện được tính trong mô hình dbt
  `int_price_indicator`, sau đó join vào `fact_price_daily` và
  `mart_stock_daily`.
- `silver.bctc_facts`: phạm vi tương lai nếu một bộ OCR/PDF sau này trích xuất dữ liệu
  từ tài liệu. Phạm vi BCTC hiện tại chỉ là metadata/tìm kiếm/xem PDF.

Cả hai khối CREATE TABLE đều đã được comment trong `warehouse/ddl/schema.sql` kèm
ghi chú.
