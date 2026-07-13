# Stock Pipeline

Cập nhật: 10/07/2026

**Stock Pipeline** là hệ thống dữ liệu phân tích chứng khoán Việt Nam theo kiến trúc **Medallion**. Dự án thu thập dữ liệu từ nhiều nguồn, chuẩn hóa qua Bronze → Silver, nạp vào PostgreSQL/TimescaleDB, dựng Gold marts bằng dbt, và cung cấp qua FastAPI + React/Vite.

Mục tiêu của dự án là xây dựng một pipeline **batch kết hợp ETL và ELT** rõ ràng, dễ backfill, dễ vận hành, và phù hợp để dùng làm đồ án đại học hoặc demo kiến trúc dữ liệu end-to-end.

## Mục lục

- [Tóm tắt kiến trúc](#tóm-tắt-kiến-trúc)
- [Phạm vi chức năng](#phạm-vi-chức-năng)
- [Tài liệu chính thức](#tài-liệu-chính-thức)
- [Cấu trúc repository](#cấu-trúc-repository)
- [Yêu cầu môi trường](#yêu-cầu-môi-trường)
- [Cài đặt nhanh](#cài-đặt-nhanh)
- [Cài đặt & vận hành](#cài-đặt--vận-hành)
- [Chạy pipeline theo từng tầng — CLI](#chạy-pipeline-theo-từng-tầng--cli)
- [Kiểm thử](#kiểm-thử)
- [Khởi động lớp phục vụ](#khởi-động-lớp-phục-vụ)
- [Vận hành Airflow](#vận-hành-airflow)
- [Khởi động nhanh hằng ngày](#khởi-động-nhanh-hằng-ngày)
- [Xử lý sự cố thường gặp](#xử-lý-sự-cố-thường-gặp)
- [Mô tả ngắn cho báo cáo](#mô-tả-ngắn-cho-báo-cáo)
- [Cheat-sheet nhanh](#cheat-sheet-nhanh)

## Tóm tắt kiến trúc

```text
Nguồn ngoài
  -> Bronze parquet/PDF tại data-lake/raw/
  -> Silver parquet tại data-lake/silver/
  -> Warehouse Loader (Python, upsert idempotent)
  -> PostgreSQL schema silver
  -> dbt schema gold
  -> FastAPI read-only
  -> React dashboard
```

Nguyên tắc vận hành:

- 4 tầng đầu **Bronze → Silver → Load → dbt** phải chạy đúng thứ tự.
- **Airflow** chỉ là bản tự động hóa theo lịch của cùng chuỗi trên.
- Backend và frontend chỉ là lớp **serving**, không thay thế pipeline dữ liệu.

## Phạm vi chức năng

- **Structured data**: giá OHLCV, chỉ số, listing, company, financial ratio, price board.
- **News pipeline**: RSS + HTML hybrid, gắn ticker và sentiment.
- **BCTC pipeline**: crawl HNX disclosure, lưu metadata và PDF.
- **Warehouse analytics**: Silver schema trong PostgreSQL, Gold marts bằng dbt.
- **Serving layer**: FastAPI read-only + React dashboard.
- **Orchestration**: Airflow 3.2 với các DAG theo lịch.

## Tài liệu chính thức

- `Docs/README.md` — mục lục tài liệu chuẩn của dự án.
- `Docs/codex-audit/Structure_data_flow.md` — luồng structured data.
- `Docs/codex-audit/News_data_flow.md` — luồng news.
- `Docs/codex-audit/BCTC_data_flow.md` — luồng BCTC PDF.
- `Docs/codex-audit/dbt_outputs_and_lineage.md` — Silver → Gold lineage, API/UI mapping.
- `Docs/codex-audit/parameters.md` — biến môi trường, config, CLI, DAG.
- `docker/airflow/README_airflow.md` — hướng dẫn Airflow.

## Cấu trúc repository

```text
stock-pipeline/
|-- ingestion/        # Bronze ingestion cho structured, news, BCTC
|-- pipeline/silver/  # Transform Bronze -> Silver parquet
|-- warehouse/        # Loader Silver -> PostgreSQL, DDL, scripts
|-- transform/dbt/    # dbt models cho Gold
|-- backend/          # FastAPI read-only API
|-- frontend/         # React/Vite dashboard
|-- docker/airflow/   # Airflow DAGs và cấu hình
|-- tests/            # Pytest contracts và transformer tests
|-- Docs/             # Tài liệu chi tiết theo luồng dữ liệu
`-- README.md         # Mục lục dự án + runbook
```

## Yêu cầu môi trường

- Python 3.12
- Node.js 18+ (hoặc tương thích với Vite)
- Docker Desktop
- PostgreSQL local với TimescaleDB extension qua `docker-compose.yml`
- PowerShell trên Windows

## Cài đặt nhanh

### Python

```powershell
pip install -r requirements.txt
pip install -r backend/requirements.txt
```

### Frontend

```powershell
cd frontend
npm install
cd ..
```

### Biến môi trường

Sao chép `.env.example` thành `.env` nếu cần.

| Biến | Mục đích |
|---|---|
| `VNSTOCK_API_KEY` | API key cho ingestion structured |
| `DATABASE_URL` | Kết nối PostgreSQL cho loader và FastAPI |
| `GOLD_DATABASE_URL` | Đọc Gold / watermark nếu cần |
| `PG_HOST` / `PG_PORT` | dbt profiles, mặc định `host.docker.internal` / `55432` |
| `STOCK_PIPELINE_ROOT` | Root repo trong container Airflow |
| `VITE_API_URL` | Base URL cho frontend |
| `STRUCTURED_DAG_UNIVERSE` | `watchlist50` hoặc `listing` |
| `STRUCTURED_MAX_TICKERS` | Giới hạn mã khi chạy full listing |
| `HNX_CRAWL_MAX_LIST_PAGES` | Số trang HNX khi crawl BCTC |
| `HNX_SSL_VERIFY` | Bật/tắt verify SSL khi crawl HNX |
| `HNX_RESUME_FROM_STATE` | Tiếp tục crawl BCTC từ state khi bị ngắt |
| `BCTC_ALLOW_EN_DOCS` | Cho phép tài liệu tiếng Anh |
| `BCTC_INGEST_ALL_CRAWLED_PDFS` | Tải toàn bộ PDF đã crawl |
| `NEWS_RATE_LIMIT_RPM` | Rate limit cho news crawl |

## Cài đặt & vận hành

> Môi trường: Windows / PowerShell. Thay `<repo-root>` bằng đường dẫn thực tế tới repository của bạn.

### 1. Sơ đồ thứ tự chạy

```text
Docker (Warehouse + Airflow)
        │
        ▼
Bronze — ingestion (notebook, backfill thủ công)
        │
        ▼
Silver — pipeline.silver.cli (theo từng dataset)
        │
        ▼
Load Silver → PostgreSQL — warehouse.loader.cli
        │
        ▼
Gold — dbt run / dbt test
        │
        ▼
Serving — FastAPI (backend) + React (frontend)
        │
        ▼
Airflow — chạy tự động theo lịch
```

4 tầng đầu (**Bronze → Silver → Load → dbt**) phải chạy đúng thứ tự vì tầng sau đọc dữ liệu do tầng trước ghi ra.

### 2. Khởi động hạ tầng Docker

Dự án có **2 stack Docker Compose riêng**:

| Stack | File compose | Chứa gì | Cổng |
|---|---|---|---|
| Warehouse | `docker-compose.yml` (gốc repo) | PostgreSQL `stock-pipeline-db` | `:55432` |
| Airflow | `docker/airflow/docker-compose.airflow.yml` | Airflow 3.2 + metadata Postgres riêng | metadata `:5433`, Web UI `:8081` |

#### 2.1 Warehouse — bắt buộc

```powershell
cd <repo-root>
docker compose up -d
```

Lần đầu tiên, hoặc khi có DDL mới:

```powershell
powershell -ExecutionPolicy Bypass -File .\warehouse\scripts\setup_db.ps1
```

> Nếu `dbt run` báo lỗi OOM hoặc `/dev/shm`, nguyên nhân thường là shared memory của Postgres warehouse. Repo đã cấu hình `shm_size: 512mb` trong `docker-compose.yml` gốc.

#### 2.2 Airflow — chỉ cần khi chạy tự động theo lịch

Lần đầu tiên:

```powershell
cd <repo-root>\docker\airflow
copy .env.airflow.example .env.airflow
docker compose -f docker-compose.airflow.yml build
docker compose -f docker-compose.airflow.yml up airflow-init
docker compose -f docker-compose.airflow.yml up -d
```

Các lần sau:

```powershell
cd <repo-root>\docker\airflow
docker compose -f docker-compose.airflow.yml up -d
```

Chi tiết biến môi trường và DAG: xem `docker/airflow/README_airflow.md`.

#### 2.3 Biến môi trường chính

Trước khi chạy loader hoặc backend:

```powershell
$env:DATABASE_URL = "postgresql://stock:stock@localhost:55432/stock_pipeline"
```

`dbt` dùng riêng `PG_HOST` / `PG_PORT` (`55432`) trong `transform/dbt/profiles.yml`.

### 3. Backfill / Ingestion — tầng Bronze

Notebook dùng để lấy dữ liệu thô từ nguồn ngoài khi backfill lần đầu hoặc chạy thử từng bước.

| Notebook | Dùng cho |
|---|---|
| `ingestion/ingest_structure_data_manager.ipynb` | `price`, `index_price`, `listing`, `company`, `financial_ratio`, `price_board` |
| `ingestion/ingest_news.ipynb` | News RSS/HTML |
| `ingestion/ingest_bctc_pdf_manager.ipynb` | `bctc_pdf_meta` (crawl HNX + tải PDF) |

#### 3.1 Structured notebook

Các biến cấu hình quan trọng:

| Biến | Giá trị | Ý nghĩa |
|---|---|---|
| `UNIVERSE_MODE` | `demo_50` / `full_listing` | 50 mã test vs full listing HOSE/HNX |
| `RUN_PROFILE` | `backfill` / `daily_incremental` | Full lịch sử vs cửa sổ incremental |
| `RUN_ONE_SHOT_PIPELINE` | `True` / `False` | Chạy 1 cell tổng vs chạy từng bước để debug |
| `PRICE_BOARD_MODE` | `watchlist_50` / full | Snapshot bảng giá 50 mã vs full listing |

**Backfill lần đầu khuyến nghị:** `UNIVERSE_MODE=full_listing`, `RUN_PROFILE=backfill`.

#### 3.2 News notebook

- `days_back=30` cho backfill / thử nghiệm rộng.
- `days_back=1` cho chạy hàng ngày hoặc kiểm tra nhanh.

#### 3.3 BCTC notebook

- `BCTC_RUN_PROFILE=backfill` để crawl sâu hơn khi cần.
- Mặc định DAG chỉ quét ít trang hơn để chạy nhanh.
- Có thể điều chỉnh `HNX_CRAWL_MAX_LIST_PAGES` khi cần mở rộng phạm vi crawl.

### 4. Chạy pipeline theo từng tầng — CLI

#### 4.1 Bronze → Silver — `pipeline.silver.cli`

Module nội bộ phía sau CLI: `price_transformer.py`, `structure_transformer.py`, `bronze_reader.py`.

```powershell
# Từng dataset structured
python -m pipeline.silver.cli --dataset price
python -m pipeline.silver.cli --dataset index_price
python -m pipeline.silver.cli --dataset listing
python -m pipeline.silver.cli --dataset company
python -m pipeline.silver.cli --dataset financial_ratio
python -m pipeline.silver.cli --dataset price_board

# Hoặc chạy tất cả dataset structured cùng lúc
python -m pipeline.silver.cli --dataset all
```

```powershell
# News và BCTC cần chỉ định đúng ngày chạy job (--run-partition)
python -m pipeline.silver.cli --dataset news --run-partition 2026-05-18
python -m pipeline.silver.cli --dataset bctc_pdf_meta --run-partition 2026-05-14
```

> Tên dataset đúng là **`index_price`** (không phải `index`) — khớp với danh sách 6 dataset structured: `price`, `index_price`, `listing`, `company`, `financial_ratio`, `price_board`.

#### 4.2 Load Silver → PostgreSQL — `warehouse.loader.cli`

```powershell
python -m warehouse.loader.cli load-silver --dataset all
```

Load một dataset riêng:

```powershell
python -m warehouse.loader.cli load-silver --dataset bctc_pdf_meta
```

#### 4.3 Silver → Gold — dbt

```powershell
dbt debug --profiles-dir transform/dbt   # kiểm tra kết nối trước khi chạy
dbt run --profiles-dir transform/dbt
```

Sau khi đã chạy `load-silver` lần đầu, các lần cập nhật dữ liệu tiếp theo chỉ cần chạy lại **`dbt run`** để Gold cập nhật.

### 5. Kiểm thử

```powershell
# Kiểm tra chất lượng dữ liệu Gold (not_null, unique, relationships…)
dbt test --profiles-dir transform/dbt
```

```powershell
# Kiểm tra DAG không lỗi import trước khi bật lại trên UI
cd <repo-root>\docker\airflow
docker compose -f docker-compose.airflow.yml run --rm airflow-scheduler airflow dags list | findstr structured
```

### 6. Khởi động lớp phục vụ

#### 6.1 Backend — FastAPI

```powershell
cd <repo-root>
$env:DATABASE_URL = "postgresql://stock:stock@localhost:55432/stock_pipeline"
python -m uvicorn backend.main:app --reload --host 127.0.0.1 --port 8000
```

Swagger docs: `http://127.0.0.1:8000/docs`

#### 6.2 Frontend — React

```powershell
cd <repo-root>\frontend
$env:VITE_API_URL = "http://127.0.0.1:8000"
npm run dev -- --host 127.0.0.1 --port 5173
```

UI: `http://127.0.0.1:5173`

### 7. Vận hành Airflow

#### 7.1 Bật DAG

DAG được tạo ở trạng thái **paused** theo mặc định — cần **unpause** trên UI (`http://localhost:8081`) thì scheduler mới chạy theo lịch.

| DAG | Lịch | Nội dung |
|---|---|---|
| `structured_daily` | T2–T6, 16:30 | price + index + price_board (watchlist 50 mã) → silver → load (7 partition) → dbt incremental |
| `structured_monthly` | Ngày 1, 17:00 | listing + company + financial_ratio (full listing) → silver → load → dbt |
| `news_daily` | Hằng ngày, 06:00 | news bronze → silver → load → dbt news marts |
| `bctc_quarterly` | 15/2, 5, 8, 11, 10:00 | BCTC crawl (10 trang gần nhất) → silver → load → `mart_bctc_documents` |
| `gold_full_refresh` | Hằng ngày, 19:00 | `dbt run --full-refresh` + `dbt test` toàn project |

Tất cả DAG: `catchup=False`, `max_active_runs=1`, task tuần tự.

#### 7.2 Trigger tay / xem log

- Trigger tay chỉ tạo run dạng `manual__...`, **không** làm phát sinh thêm lịch cron.
- Trước khi trigger tay, nếu đang có run `scheduled__...` bị kẹt (queued/running), nên mark failed run đó trước.
- Log từng task xem trực tiếp trên Grid view của UI.

#### 7.3 Nạp lại DAG sau khi sửa code

```powershell
cd <repo-root>\docker\airflow
docker compose -f docker-compose.airflow.yml up -d --force-recreate airflow-scheduler airflow-dag-processor airflow-apiserver
docker compose -f docker-compose.airflow.yml restart airflow-dag-processor airflow-scheduler
```

Sau đó kiểm tra lại bằng lệnh kiểm tra DAG không lỗi import trước khi bật lại trên UI.

### 8. Khởi động nhanh hằng ngày

Khi đã setup xong lần đầu, thường chỉ cần chạy song song 4 terminal:

```powershell
# Terminal 1 — Warehouse DB
cd <repo-root>
docker compose up -d

# Terminal 2 — Airflow (bỏ qua nếu chỉ cần chạy tay/dev)
cd <repo-root>\docker\airflow
docker compose -f docker-compose.airflow.yml up -d

# Terminal 3 — Backend API
cd <repo-root>
$env:DATABASE_URL = "postgresql://stock:stock@localhost:55432/stock_pipeline"
uvicorn backend.main:app --reload --port 8000

# Terminal 4 — Frontend
cd <repo-root>\frontend
npm run dev
```

Nếu có dữ liệu mới cần cập nhật Gold: chạy lại `dbt run` rồi refresh trình duyệt.

### 9. Xử lý sự cố thường gặp

| Triệu chứng | Nguyên nhân | Cách xử lý |
|---|---|---|
| Airflow đứng lại sau task Bronze | DAG đang **paused** | Unpause DAG trên UI; đợi scheduler tự schedule task downstream |
| 2 run hiện trên Grid (manual + scheduled) | Bù slot lịch cũ + vừa trigger tay | Mark failed run `scheduled__` đang kẹt trước khi trigger tay |
| Kho tin trống / API trả lỗi 500 | SQL lỗi hoặc chưa `load-silver` / `dbt run` cho news | Xem log backend; chạy lại chuỗi `news_daily` |
| Tin tức lệch ngày khi lọc | Nhầm UTC vs giờ Việt Nam | API và UI đều dùng `Asia/Ho_Chi_Minh` |
| `dbt run` fail OOM / lỗi `/dev/shm` | Postgres warehouse thiếu shared memory | Đảm bảo `shm_size: 512mb` có trong `docker-compose.yml` gốc |
| BCTC PDF trả 404 | API host không có file local (`pdf_path`) | Chạy API cùng máy với `data-lake`, hoặc mount volume dùng chung |
| Giá chỉ thấy 50 mã dù đã backfill | DAG daily mặc định chỉ update watchlist 50 mã | Đây là thiết kế; lịch sử full listing vẫn còn nguyên từ lần backfill notebook |

## Mô tả ngắn cho báo cáo

> Hệ thống pipeline chứng khoán Việt Nam theo kiến trúc Medallion, ingest dữ liệu từ nhiều nguồn, chuẩn hóa qua Bronze/Silver, xử lý phân tích bằng dbt trên PostgreSQL, và phục vụ qua API + dashboard.

## Cheat-sheet nhanh

```powershell
# Hạ tầng
cd <repo-root>
docker compose up -d
cd <repo-root>\docker\airflow; docker compose -f docker-compose.airflow.yml up -d; cd ..\..

# Backfill Bronze -> chạy notebook
# ingestion/ingest_structure_data_manager.ipynb
# ingestion/ingest_news.ipynb
# ingestion/ingest_bctc_pdf_manager.ipynb

# Bronze -> Silver
python -m pipeline.silver.cli --dataset all
python -m pipeline.silver.cli --dataset news --run-partition <YYYY-MM-DD>
python -m pipeline.silver.cli --dataset bctc_pdf_meta --run-partition <YYYY-MM-DD>

# Silver -> PostgreSQL
python -m warehouse.loader.cli load-silver --dataset all

# Silver.* -> Gold
dbt debug --profiles-dir transform/dbt
dbt run --profiles-dir transform/dbt
dbt test --profiles-dir transform/dbt

# Serving
$env:DATABASE_URL = "postgresql://stock:stock@localhost:55432/stock_pipeline"
uvicorn backend.main:app --reload --port 8000
cd <repo-root>\frontend; npm run dev
```
