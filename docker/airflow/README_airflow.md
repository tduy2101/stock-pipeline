# Airflow orchestration — stock-pipeline

Apache Airflow **3.2.2** (LocalExecutor) chạy 5 DAG orchestrate luồng Medallion:
Bronze → Silver → PostgreSQL `silver` → dbt `gold`.

## Kiến trúc

```text
Airflow (docker/airflow)
  ├── metadata Postgres :5433
  └── tasks → PYTHONPATH=/opt/stock-pipeline
        ├── ingestion.* (bronze)
        ├── pipeline.silver.cli (silver)
        ├── warehouse.loader.cli (load-silver)
        └── dbt run/test (gold)

Pipeline warehouse (root docker-compose.yml)
  └── stock-pipeline-db :55432  ← host.docker.internal từ container Airflow
```

**Hai PostgreSQL tách biệt:**

| DB | Port (host) | Mục đích |
| --- | --- | --- |
| `airflow-postgres` | 5433 | Metadata Airflow |
| `stock-pipeline-db` | 55432 | Warehouse silver/gold |

## DAGs

| DAG | Schedule (ICT) | Pipeline | dbt subset |
| --- | --- | --- | --- |
| `structured_daily` | T2–T6 16:30 | bronze price+index+price_board (50 mã) → silver (3 datasets) → load → dbt | `+mart_stock_daily +mart_price_board +mart_market_overview` |
| `structured_monthly` | Ngày 1 hàng tháng 17:00 | bronze listing+company+financial_ratio (full HOSE/HNX) → silver (3 datasets) → load → dbt | `+mart_financial_summary +mart_company_profile` |
| `news_daily` | Daily 06:00 | bronze news → silver `--run-partition` → load → dbt | `+mart_stock_news_signal +fact_news_article` |
| `bctc_weekly` | T7 10:00 | bronze BCTC → silver `bctc_pdf_meta` → load → dbt | `+mart_bctc_documents` |
| `gold_full_refresh` | Daily 19:00 | `dbt run` + `dbt test` full project | toàn bộ |

Tất cả DAG: `catchup=False`, task tuần tự (không parallel cùng nguồn API).

## Airflow 3.x vs 2.8

| Thành phần | Airflow 2.8 | Airflow 3.2 (repo này) |
| --- | --- | --- |
| Web UI | `webserver` | `api-server` (port 8081→8080) |
| Parse DAG | scheduler inline | `dag-processor` service riêng |
| Test DAG | `airflow dags test` | `airflow dags test <dag_id> <logical_date>` |

## Yêu cầu

- Docker Desktop (Windows), RAM khuyến nghị 8GB+
- Pipeline Postgres đang chạy: `docker compose up -d` từ **repo root**
- File `../../.env` có `VNSTOCK_API_KEY` (structured/financial ratio)
- Copy env Airflow: `cp .env.airflow.example .env.airflow`

## First-time setup

```powershell
# 1. Warehouse Postgres (repo root)
cd D:\WorkSpace\Đồ Án 2\stock-pipeline
docker compose up -d

# 2. Airflow stack
cd docker\airflow
copy .env.airflow.example .env.airflow
# Chỉnh VNSTOCK_API_KEY trong ..\..\.env nếu chưa có

docker compose -f docker-compose.airflow.yml build
docker compose -f docker-compose.airflow.yml up airflow-init
docker compose -f docker-compose.airflow.yml up -d
```

UI: http://localhost:8081 — user `airflow` / password `airflow` (mặc định).

DAGs được tạo **paused**; bật từng DAG trên UI khi sẵn sàng.

## Verify (acceptance)

```powershell
cd docker\airflow

# UI + services
docker compose -f docker-compose.airflow.yml ps

# Import ingestion modules
docker compose -f docker-compose.airflow.yml run --rm airflow-cli python -c ^
  "import ingestion.structure_data, ingestion.unstructured_data, ingestion.semi_structure_data; print('OK')"

# Warehouse connectivity
docker compose -f docker-compose.airflow.yml run --rm airflow-cli ^
  python -m warehouse.loader.cli load-silver --dataset listing

# dbt subset
docker compose -f docker-compose.airflow.yml run --rm airflow-cli ^
  dbt run --project-dir transform/dbt --profiles-dir transform/dbt --select +mart_price_board

# One-shot DAG test (Airflow 3)
docker compose -f docker-compose.airflow.yml run --rm airflow-scheduler ^
  airflow dags test structured_daily 2026-06-06
```

## Manual trigger / backfill một ngày

1. UI → DAG → **Trigger DAG** (logical date = ngày cần chạy).
2. Hoặc CLI:

```powershell
docker compose -f docker-compose.airflow.yml run --rm airflow-scheduler ^
  airflow dags test news_daily 2026-06-06
```

**Lưu ý:** News/BCTC silver dùng `run_partition` từ XCom bronze (ngày ingest thực tế), không hardcode `{{ ds }}`.

## Config DAG = class defaults (KHÔNG notebook backfill)

| Luồng | Notebook backfill | DAG |
| --- | --- | --- |
| Structured | `UNIVERSE_MODE=full_listing`, `RUN_PROFILE=backfill`, `PRICE_BOARD_MODE=watchlist_50` | Tách thành `structured_daily` (price, index, price_board incremental) và `structured_monthly` (listing, company, financial_ratio full HOSE/HNX) |
| News | `days_back=30` | `NewsIngestionConfig()` — `days_back=1` |
| BCTC | `hnx_max_list_pages=500` | `SemiStructuredIngestionConfig()` — `hnx_max_list_pages=10` |

Không set `HNX_CRAWL_MAX_LIST_PAGES=500` trong env Airflow trừ khi cố ý backfill thủ công.

## Bootstrap & thời gian chạy ước lượng

**Structured daily (`structured_daily`):**

- Chỉ price + index + price_board, mặc định 50 mã (watchlist50): **~5–15 phút** (@ 50 rpm).
- listing & company **không** chạy daily nữa — chuyển sang `structured_monthly`.
- Lần đầu không có Bronze: `bootstrap_full_history_if_missing=False` (DAG) → mã thiếu chỉ kéo ~2 ngày, không full 5 năm. Backfill 5 năm chạy từ notebook.
- Marker: `data-lake/raw/Structure_Data/price/_full_bootstrap_done.json`.

**Structured monthly (`structured_monthly`):** full HOSE/HNX listing + company + financial_ratio (~700–1000 mã) → **~30–90 phút** (@ 50 rpm). Chạy ngày 1 hàng tháng.

**News:** ~5–15 phút (20 rpm).

**BCTC weekly:** ~10–30 phút (10 trang HNX, 10 rpm).

**Gold full refresh:** phụ thuộc kích thước warehouse (~2–10 phút local).

## Audit

- Silver runs: `data-lake/silver/<dataset>/_runs.jsonl`
- Warehouse load: bảng `silver.load_audit` (PostgreSQL)
- Airflow logs: `docker/airflow/logs/`

## Troubleshooting

### Rate limit / vnstock

- Structured: `rate_limit_rpm=50` in config (conservative vs Community 60 rpm).
- **Guest (no API key):** hard limit **20 req/min** — `structured_monthly` sẽ fail sớm ở bước company/financial_ratio (~1000 mã).
- **`VNSTOCK_API_KEY`:** đặt trong repo root `.env` dạng `VNSTOCK_API_KEY=your_key` (**không** có khoảng trắng quanh `=`).
- **`STRUCTURED_DAG_UNIVERSE=watchlist50`** (mặc định DAG): 50 mã cố định `DEFAULT_PRICE_BOARD_TICKERS` (VCB, FPT, HPG, …). **`listing`**: full HOSE/HNX (~1051 mã); kèm **`STRUCTURED_MAX_TICKERS=N`** nếu chỉ muốn N mã đầu alphabet (khác watchlist).
- **Quan trọng:** không để dòng `VNSTOCK_API_KEY=` (rỗng) trong `docker/airflow/.env.airflow` — file này load **sau** `../../.env` và sẽ **ghi đè** key thật thành rỗng.
- Sau khi sửa env: `docker compose -f docker-compose.airflow.yml up -d --force-recreate airflow-scheduler airflow-apiserver airflow-dag-processor`

### dbt / PostgreSQL từ container

- `DATABASE_URL` và dbt dùng `PG_HOST=host.docker.internal`, `PG_PORT=55432`.
- Đảm bảo `docker compose up -d` (warehouse) trước khi chạy load/dbt tasks.
- Nếu `host.docker.internal` không resolve: thêm `extra_hosts` đã có trong compose; hoặc dùng IP máy host.

### Silver `--run-partition`

- News/BCTC **bắt buộc** `--run-partition YYYY-MM-DD` khớp partition Bronze ngày chạy.
- Structured silver: gọi **từng dataset** (`price`, `index_price`, `listing`, `company`, `price_board`) — CLI không nhận comma list.

### price_board null keys

- Silver strict mode (`--strict`) fail nếu validation lỗi — xem log task `silver_structured`.
- Bronze mặc định: 1 `snapshot_at`/run, full listing batched.

### RAM 8GB

- Không bật đồng thời nhiều DAG nặng (structured + BCTC).
- Image đã bỏ Jupyter/matplotlib để giảm size; build lần đầu ~5–10 phút.

## File layout

```text
docker/airflow/
  Dockerfile.airflow
  docker-compose.airflow.yml
  requirements-airflow.txt
  .env.airflow.example
  dags/
    common/callbacks.py, tasks.py
    structured_daily.py
    structured_monthly.py
    news_daily.py
    bctc_weekly.py
    gold_full_refresh.py
  logs/
  plugins/
  config/
```

Volumes mount:

- `../../data-lake` → `/opt/airflow/data-lake` (Bronze/Silver persistent)
- `../../` → `/opt/stock-pipeline` (code + PYTHONPATH)

## Optional: shared Docker network

Thay `host.docker.internal` bằng network chung:

1. Thêm vào root `docker-compose.yml`:

```yaml
networks:
  stock_net:
    name: stock_pipeline_net
```

2. Gắn `postgres` service vào `stock_net`.
3. Trong Airflow compose, `external: true` network `stock_pipeline_net` và set `PG_HOST=stock-pipeline-db`.

Mặc định repo dùng `host.docker.internal` (đơn giản nhất trên Windows).

## Secrets

- `VNSTOCK_API_KEY`, `DATABASE_URL`: repo root `.env` + `docker/airflow/.env.airflow`
- Không commit secrets; `.env` / `.env.airflow` gitignore
