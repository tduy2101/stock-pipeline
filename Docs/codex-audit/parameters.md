## Tổng quan

Tham số trong project chia làm **4 lớp**:

1. **Biến môi trường** — đổi không cần sửa Python (chủ yếu Airflow `.env`)
2. **Class config** — default trong code (`IngestionConfig`, …)
3. **CLI** — tham số khi chạy tay (`--dataset`, `--latest-partitions`, …)
4. **DAG Airflow** — **ghi đè** config + chọn subset dataset/dbt/load

---

## 1. Biến môi trường (thay đổi được từ bên ngoài)

| Biến | Mặc định / vai trò | Ảnh hưởng |
|------|-------------------|-----------|
| `VNSTOCK_API_KEY` | `.env` gốc | API vnstock (Bronze structured) |
| `STRUCTURED_DAG_UNIVERSE` | `watchlist50` | `watchlist50` = 50 mã cố định; `listing` = full HOSE/HNX stock |
| `STRUCTURED_MAX_TICKERS` | (không set) | Cap số mã khi `universe=listing` (dev/test) |
| `DATABASE_URL` | `postgresql://stock:stock@localhost:55432/stock_pipeline` | Loader, FastAPI, Silver đọc Gold watermark |
| `GOLD_DATABASE_URL` | (fallback `DATABASE_URL`) | Silver price đọc watermark Gold |
| `PG_HOST`, `PG_PORT` | `localhost`, `55432` | dbt `profiles.yml` |
| `STOCK_PIPELINE_ROOT` | `/opt/stock-pipeline` (Docker) | Airflow chạy CLI từ đâu |
| `HNX_CRAWL_MAX_LIST_PAGES` | (không set → config 10) | BCTC: số trang crawl HNX |
| `HNX_DISCLOSURE_API_URL` | (không set) | URL API HNX tùy chọn |
| `HNX_SSL_VERIFY` | `0` thường dùng dev | SSL khi crawl HNX |
| `HNX_RESUME_FROM_STATE` | `0` | Tiếp tục crawl từ state |
| `BCTC_ALLOW_EN_DOCS` | `0` | Cho phép tài liệu EN |
| `BCTC_INGEST_ALL_CRAWLED_PDFS` | `0` | Tải mọi PDF thay vì chỉ BCTC VI |
| `BCTC_DOWNLOAD_CONNECT_TIMEOUT` | code default | Timeout download PDF |
| `BCTC_DOWNLOAD_READ_TIMEOUT` | code default | Timeout đọc |
| `BCTC_DOWNLOAD_READ_TIMEOUT_HNX_OWA` | 180s | Timeout riêng `owa.hnx.vn` |

---

## 2. Bronze structured — `IngestionConfig`  
(`ingestion/structure_data/config.py`)

### Universe & mã

| Tham số | Default code | Ghi chú |
|---------|--------------|---------|
| `tickers` | 20 mã demo | Notebook/backfill có thể đổi |
| `DEFAULT_PRICE_BOARD_TICKERS` | **50 mã** | DAG `watchlist50` dùng list này |
| `index_tickers` | 5 chỉ số VNINDEX… | |
| `use_listing_as_universe` | `True` | DAG daily đặt `False` (watchlist50) |
| `listing_exchange_filter` | `["HOSE","HNX"]` | |
| `listing_security_type_filter` | `["stock"]` | ~703 mã CP (đã fix) |
| `listing_max_tickers` | `None` | Env `STRUCTURED_MAX_TICKERS` |
| `max_tickers_per_run` | `50` | DAG watchlist50 → `len(tickers)=50` |

### Nguồn & rate limit

| Tham số | Default |
|---------|---------|
| `primary_source` / `fallback_source` | `kbs` / `vci` |
| `rate_limit_rpm` | `50` |
| `inter_request_delay_sec` | `0.5` |
| `api_retry_max_attempts` | `4` |
| `api_retry_base_delay_sec` | `1.5` |

### Incremental / backfill

| Tham số | Default code | DAG daily ghi đè |
|---------|--------------|------------------|
| `years_back` | `5` | — |
| `use_incremental_window` | `True` | — |
| `incremental_window_days` | **`10`** | **`2`** |
| `bootstrap_full_history_if_missing` | `True` | **`False`** |
| `use_bronze_ticker_watermark` | `False` | **`True`** |
| `full_bootstrap_once_then_incremental` | `False` | — |
| `run_partition` | `None` | — |

### Batch & price board

| Tham số | Default |
|---------|---------|
| `price_batch_size` | `100` |
| `price_board_batch_size` | `50` |
| `delay_between_batches_sec` | `5.0` |
| `delay_between_categories_sec` | **`30`** → DAG: **`5`** |
| `price_board_use_listing_universe` | `True` | DAG watchlist50: **`False`** |
| `price_board_tickers` | 50 mã watchlist | |
| `price_board_max_tickers` | `5000` |
| `price_board_batched` | `True` |

### QC OHLCV

| Tham số | Default |
|---------|---------|
| `min_ohlcv_rows_stock` | `50` (backfill) |
| `min_ohlcv_rows_stock_incremental` | `5` |
| `min_ohlcv_rows_index` | `100` / `5` incremental | |

### Financial ratio

| Tham số | Default |
|---------|---------|
| `financial_ratio_exchange_filter` | `["HOSE","HNX"]` |
| `financial_ratio_retry_*` | 2 lần, disable source on error | |

### Pipeline flags (`run_structure_ingestion_pipeline`)

| Tham số | Default full pipeline | DAG daily | DAG monthly |
|---------|----------------------|-----------|-------------|
| `include_prices` | `True` | `True` | **`False`** |
| `include_indices` | `True` | `True` | **`False`** |
| `include_listing` | `True` | **`False`** | `True` |
| `include_company` | `True` | **`False`** | `True` |
| `include_financial_ratio` | `False` | **`False`** | `True` |
| `include_price_board` | `True` | `True` | **`False`** |

---

## 3. Silver — `SilverConfig` + CLI  
(`pipeline/silver/config.py`, `cli.py`)

| Tham số | Cách đổi |
|---------|----------|
| `repo_root`, `data_lake_dir`, `bronze_dir`, `silver_dir` | Sửa `SilverConfig` |
| `--dataset` | `all`, `price`, `index_price`, `listing`, `company`, `financial_ratio`, `price_board`, `news`, `bctc_pdf_meta` |
| `--run-partition` | Ép partition Bronze (news, BCTC, backfill tháng) |
| `--price-run-partition` / `--index-run-partition` | Ép tháng Bronze giá CP/chỉ số |
| `--strict` | News: fail nếu validation ERROR |

**Incremental Silver (trong code, không phải CLI):**
- Giá CP: watermark = max(Silver partition, Gold `fact_price_daily` / `mart_stock_daily`)
- Giá chỉ số: chỉ max Silver
- Sau success: cập nhật `_watermark.json` (chỉ price + index)

**DAG Silver:** không truyền partition — chạy incremental mặc định; news/BCTC lấy partition từ XCom.

---

## 4. Loader — `warehouse/loader/cli.py`

| Tham số | Default | DAG |
|---------|---------|-----|
| `--dataset` | `all` | Xem bảng DAG bên dưới |
| `--latest-partitions N` | `None` (full scan) | Daily: **`N=7`** (price, index_price, price_board) |
| `DATABASE_URL` | env | |
| `batch_size` upsert | **`2000`** (hardcode `silver_loader.py`) | |

---

## 5. dbt Gold

### Project (`dbt_project.yml`)

| Folder | Materialization mặc định |
|--------|-------------------------|
| `staging/` | `view` |
| `intermediate/` | `table` |
| `marts/` | `table` |

### Profile (`profiles.yml`)

| Tham số | Default |
|---------|---------|
| `PG_HOST`, `PG_PORT` | env |
| `schema` | **`gold`** |
| `threads` | `4` |

### Ghi đè trong từng model SQL

| Model | Config quan trọng |
|-------|-------------------|
| `int_price_indicator` | incremental, delete+insert, unique `ticker+trading_date`, **lookback 90 ngày** |
| `fact_price_daily`, `mart_stock_daily`, `mart_market_overview` | incremental tương tự |
| `fact_index_daily`, `mart_price_board`, dim, nhiều mart | `table` (rebuild) |
| `stg_bctc_pdf_meta` | `ephemeral` |

### Hằng số selector trong `tasks.py` (đổi = sửa code DAG)

| Hằng | Nội dung |
|------|----------|
| `DBT_STRUCTURED_SELECT` | Daily incremental subset |
| `DBT_STRUCTURED_MONTHLY_SELECT` | `+mart_financial_summary +mart_company_profile` |
| `DBT_NEWS_SELECT` | `+mart_stock_news_signal +fact_news_article` |
| `DBT_BCTC_SELECT` | `+mart_bctc_documents` |
| `DBT_STRUCTURED_SELECT_FULL` | Legacy full rebuild (manual) |

---

## 6. News — `NewsIngestionConfig`

| Tham số | Default | DAG `news_daily` |
|---------|---------|------------------|
| `days_back` | `1` | Giữ default |
| `rate_limit_rpm` | `20` | — |
| `max_articles_per_source` | `200` | — |
| `enable_rss` / `enable_html` | `True` | — |
| `enable_ticker_match` | `True` | — |
| `use_listing_tickers` | `False` | — |
| `strict` (Silver) | — | **`True`** |

---

## 7. BCTC — `SemiStructuredIngestionConfig`

| Tham số | Default | DAG `bctc_quarterly` |
|---------|---------|----------------------|
| `hnx_max_list_pages` | **`10`** | Default (notebook backfill có thể 500) |
| `rate_limit_rpm` | `10` | — |
| `ingest_only_financial_statement_vi` | `True` | — |
| `hnx_verify_ssl` | `False` | — |
| `run_partition` | `None` | Set **`date.today()`** trong task |

---

## 8. Backend — `backend/config.py`

| Tham số | Default |
|---------|---------|
| `DATABASE_URL` | env |
| `DEFAULT_PAGE_SIZE` | `100` |
| `MAX_PAGE_SIZE` | `500` |
| `GOLD_SCHEMA` | `"gold"` |

---

## 9. DAG đã thay đổi gì so với default code?

### `structured_daily` (T2–T6 16:30)

| Khía cạnh | Default code | DAG đã set |
|-----------|--------------|------------|
| Universe | listing HOSE/HNX stock | **`watchlist50`** (50 mã cố định) |
| `use_listing_as_universe` | `True` | **`False`** |
| `price_board_use_listing_universe` | `True` | **`False`** |
| `incremental_window_days` | `10` | **`2`** |
| `bootstrap_full_history_if_missing` | `True` | **`False`** |
| `use_bronze_ticker_watermark` | `False` | **`True`** |
| `delay_between_categories_sec` | `30` | **`5`** |
| Pipeline includes | full 6 nhóm | **Chỉ price + index + price_board** |
| Silver | — | `price` → `index_price` → `price_board` |
| Load | full parquet | **`latest_partitions=7`**, 3 dataset |
| dbt | full project | **Subset incremental** (`DBT_STRUCTURED_SELECT`) |
| Retries bronze | — | **2**, delay 15 phút |

**Đổi universe không sửa code:** `STRUCTURED_DAG_UNIVERSE=listing` (+ `STRUCTURED_MAX_TICKERS` tùy chọn).

---

### `structured_monthly` (ngày 1, 17:00)

| Khía cạnh | DAG đã set |
|-----------|------------|
| Universe | **Luôn** HOSE/HNX **stock** (bỏ qua `STRUCTURED_DAG_UNIVERSE`) |
| `listing_security_type_filter` | **`["stock"]`** |
| `bootstrap_full_history_if_missing` | **`False`** |
| `delay_between_categories_sec` | **`5`** |
| Pipeline | **listing + company + financial_ratio** (bỏ price/index/board) |
| Load | full scan 3 dataset |
| dbt | `+mart_financial_summary +mart_company_profile` |

---

### `news_daily` (06:00)

| Khía cạnh | DAG |
|-----------|-----|
| Bronze | `NewsIngestionConfig()` — `days_back=1` |
| Silver | `--strict`, partition từ XCom |
| Load | `news` only |
| dbt | news marts |

---

### `bctc_quarterly` (15/2,5,8,11 10:00)

| Khía cạnh | DAG |
|-----------|-----|
| Bronze | `hnx_max_list_pages=10`, `run_partition=today` |
| Silver/Load/dbt | `bctc_pdf_meta` → `+mart_bctc_documents` |

---

### `gold_full_refresh` (19:00)

| Khía cạnh | DAG |
|-----------|-----|
| dbt | **`run --full-refresh`** toàn project + **`dbt test`** |

---

### Task có trong code nhưng **không có DAG riêng**

- `ingest_financial_ratio()` — weekly financial ratio (load tickers từ listing bronze); gọi tay hoặc DAG tương lai.

---

## 10. Bảng tóm: chỗ nào đổi không cần sửa Python?

| Mục tiêu | Cách đổi |
|----------|----------|
| 50 mã ↔ full thị trường daily | `STRUCTURED_DAG_UNIVERSE` |
| Giới hạn mã khi full listing | `STRUCTURED_MAX_TICKERS` |
| DB / dbt host | `DATABASE_URL`, `PG_HOST`, `PG_PORT` |
| API vnstock | `VNSTOCK_API_KEY` |
| BCTC crawl sâu | `HNX_CRAWL_MAX_LIST_PAGES` |
| Backfill/manual | Notebook + CLI `--dataset`, `--run-partition`, loader không `--latest-partitions` |
| dbt subset daily | Sửa `DBT_STRUCTURED_SELECT` trong `tasks.py` |
| Incremental lookback 90 ngày | Sửa SQL `int_price_indicator.sql` |
| Upsert batch size | `silver_loader.py` `batch_size=2000` |

---

## Một câu cho bảo vệ

> *“Default trong `IngestionConfig` hướng backfill full universe; DAG hàng ngày ghi đè thành 50 mã watchlist, incremental 2 ngày, watermark theo từng mã, chỉ bronze price/index/board, load 7 partition, dbt incremental subset; DAG tháng refresh listing/company/financial_ratio; 19h full-refresh dbt. Phần còn lại đổi qua env hoặc CLI mà không đụng logic core.”*

Nếu cần, tôi có thể xuất bảng này sang LaTeX (mục phụ lục “Tham số vận hành”) cho Chương 3/4.