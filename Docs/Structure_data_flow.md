# Luồng dữ liệu có cấu trúc (Structure Data)

Cập nhật: 2026-06-01

Tài liệu mô tả **toàn bộ** luồng dữ liệu có cấu trúc: Bronze → Silver parquet → PostgreSQL `silver` → Gold (dbt) → FastAPI/Frontend.

```text
vnstock → Bronze → Silver parquet → silver.* (PG) → gold.* (dbt) → FastAPI → React dashboard
```

**Tài liệu luồng khác:** [News](News_data_flow.md) · [BCTC](BCTC_data_flow.md) · [README](../README.md)

---

## 1. Bối cảnh dự án (tóm tắt)

Dự án **stock-pipeline** xây dựng pipeline và ứng dụng tra cứu thị trường chứng khoán Việt Nam. Luồng structured data nằm tại:

| Thành phần | Vai trò |
|---|---|
| `ingestion/structure_data/` | Module Python: fetch vnstock, ghi Bronze |
| `ingestion/ingest_structure_data_manager.ipynb` | Notebook điều phối: reload module, gán mã, profile backfill/incremental, chạy từng bước hoặc pipeline |
| `data-lake/raw/Structure_Data/` | Bronze root (gitignore) |
| `data-lake/silver/<dataset>/` | Silver parquet sạch |
| `pipeline/silver/` | Bronze → Silver; **cập nhật** `_watermark.json` sau transform OHLCV thành công |
| `warehouse/loader/` | Silver parquet → PostgreSQL upsert idempotent |
| `transform/dbt/` | `silver.*` → staging/intermediate/marts trong schema `gold` |
| `backend/` | FastAPI read-only trên `gold.*` |
| `frontend/` | React dashboard — gọi API, hiển thị output structured |

Bronze **không** load thẳng vào PostgreSQL. Gold **không** đọc file parquet — chỉ đọc bảng `silver` trong PostgreSQL. Frontend **không** đọc DB hay file parquet.

---

## 2. Cấu trúc code `ingestion/`

```text
ingestion/
├── ingest_structure_data_manager.ipynb   # Điều phối chính (Jupyter)
├── structure_data/
│   ├── __init__.py          # Export API công khai
│   ├── config.py            # IngestionConfig (tickers, nguồn, incremental, paths)
│   ├── common.py            # Rate limit, retry, parquet I/O, watermark đọc, merge tháng
│   ├── price_ingestor.py    # OHLCV cổ phiếu (Quote.history)
│   ├── index_ingestor.py    # OHLCV chỉ số (Quote.history)
│   ├── stock_info_ingestor.py  # listing, company, financial_ratio, price_board
│   └── pipeline.py          # run_structure_ingestion_pipeline, full pipeline
└── common/                  # Tiện ích chung ingestion (nếu có)
```

**Entry points chính:**

| Hàm | Mô tả |
|---|---|
| `run_structure_ingestion_pipeline(cfg)` | Pipeline mặc định: price → index → listing → company → price_board; **tắt** `financial_ratio` |
| `run_structure_full_ingestion_pipeline(cfg)` | Giống trên + bật `financial_ratio` |
| `run_financial_ratio_ingestion_pipeline(cfg)` | Chỉ financial ratio (schedule riêng) |
| `ingest_prices` / `ingest_indices` / … | Chạy từng dataset độc lập |

Giữa các nhóm API, pipeline nghỉ `delay_between_categories_sec` (mặc định 30s) để giảm rate limit / lỗi mạng.

---

## 3. Nguồn dữ liệu và công cụ

### 3.1. Nguồn

| Dataset | API vnstock | Class / method |
|---|---|---|
| **price** | Lịch sử OHLCV cổ phiếu | `Quote(source=…, symbol=…).history(start, end, interval="1D")` |
| **index** | Lịch sử OHLCV chỉ số | `Quote` (cùng API, `instrument_type="index"`) |
| **listing** | Danh sách mã theo sàn | `Listing(source=…).symbols_by_exchange(…)` hoặc fallback `all_symbols()` |
| **company** | Thông tin công ty | `Company(source=…, symbol=…).overview()` hoặc `.profile()` |
| **financial_ratio** | Chỉ số tài chính | `Finance(source=…, symbol=…).ratio(period="quarter"\|"year")` |
| **price_board** | Bảng giá snapshot | `Trading(source=…).price_board(symbols_list=…)` |

### 3.2. Công cụ và cấu hình

- **Thư viện:** [vnstock](https://github.com/thinh-vu/vnstock) (Python).
- **API key:** biến môi trường `VNSTOCK_API_KEY` trong `stock-pipeline/.env`; `register_vnstock_api_key_from_env()` gọi `vnstock.register_user(key)` khi khởi động notebook/script.
- **Nguồn ưu tiên / dự phòng:** `primary_source` → `fallback_source` (mặc định **KBS** rồi **VCI**), qua `IngestionConfig.resolved_data_sources()`.
- **Rate limit:** `rate_limit_rpm` (mặc định 10 req/phút) + `inter_request_delay_sec` giữa từng mã (company, financial_ratio).
- **Retry:** `call_with_retry` — exponential backoff cho lỗi mạng/timeout; OHLCV thử lần lượt từng nguồn đến khi QC đạt.
- **QC OHLCV (Bronze):** `validate_ohlcv_frame` — số dòng tối thiểu, cột ngày/close, tỷ lệ high &lt; low; ngưỡng khác nhau cho full vs incremental.

**Phạm vi mã (notebook hiện tại):**

| Dataset | Phạm vi mặc định / notebook |
|---|---|
| **price** | `IngestionConfig`: 20 mã VN30 mẫu; notebook gán **50 mã** (VN30 + 30 mã đa ngành), `max_tickers_per_run = len(tickers)` — có thể mở rộng lên toàn universe từ **listing** |
| **index** | `VNINDEX`, `VN30`, `HNXINDEX`, `HNX30`, `UPCOMINDEX` |
| **listing** | Toàn bộ mã trên HNX, HOSE, UPCOM (~3000+ dòng, snapshot) |
| **company** | Cùng danh sách `cfg.tickers` (tối đa `max_tickers_per_run`) |
| **financial_ratio** | Cùng danh sách tickers (pipeline riêng hoặc full pipeline) |
| **price_board** | Batch một lần cho toàn bộ `cfg.tickers` trong run |

---

## 4. Điều phối chạy (notebook & CLI)

### 4.1. Notebook `ingest_structure_data_manager.ipynb`

1. Bật UTF-8, `importlib.reload` các submodule `structure_data` (tránh cache kernel cũ).
2. `register_vnstock_api_key_from_env`, `configure_logging()`, tạo `cfg = IngestionConfig()`.
3. Gán `cfg.tickers`, `cfg.index_tickers`, `cfg.run_partition` (timestamp mỗi lần chạy Jupyter).
4. Chọn **`RUN_PROFILE`**:
   - **`backfill`:** `use_incremental_window=False` → fetch full ~`years_back` năm (mặc định 5).
   - **`daily_incremental`:** `use_incremental_window=True`, `incremental_window_days=1` → incremental theo watermark/cửa sổ.
5. Chạy từng ô `ingest_*` hoặc một lệnh `run_structure_ingestion_pipeline(cfg)`.

`run_date` / `run_id` = `cfg.run_partition` (ví dụ `2026-05-18T171513`) hoặc `date.today().isoformat()` nếu không set — dùng cho metadata snapshot (`ingested_at`, `snapshot_date`), **không** phải partition chính của OHLCV (OHLCV partition theo **tháng giao dịch**).

### 4.2. Chạy từ Python (không notebook)

```powershell
@'
from ingestion.structure_data import IngestionConfig, run_structure_ingestion_pipeline
cfg = IngestionConfig()
print(run_structure_ingestion_pipeline(cfg))
'@ | python -
```

Financial ratio đầy đủ: `run_structure_full_ingestion_pipeline(cfg)` hoặc `run_financial_ratio_ingestion_pipeline(cfg)`.

---

## 5. Cơ chế lấy dữ liệu

### 5.1. OHLCV (`price`, `index`) — partition theo tháng giao dịch

**Đường dẫn:** `price|index/year=<YYYY>/month=<MM>/<TICKER|INDEX>.parquet`

**Ghi file:** fetch theo khoảng ngày → gom theo tháng → **merge** với parquet tháng đã có (nếu có) → dedupe `ticker` + `trading_date` (giữ bản `fetched_at` mới nhất) → **ghi đè** file tháng. Đây là incremental theo **khoảng ngày fetch**, không tạo partition mới theo ngày chạy job.

**Chia cửa sổ API:** nếu `(end - start) > 366` ngày, fetch theo từng năm dương lịch (tránh timeout/ giới hạn API).

#### A. Backfill / lần đầu (`use_incremental_window=False`)

- `start_date` = hôm nay − `years_back` năm (mặc định 5), `end_date` = hôm nay.
- Mode log: `full_5y` (tương ứng `years_back`).
- QC tối thiểu: stock `min_ohlcv_rows_stock` (50), index `min_ohlcv_rows_index` (100).

#### B. Incremental (`use_incremental_window=True`) — profile `daily_incremental`

Thứ tự quyết định khoảng fetch (per symbol/index), trong `price_ingestor._resolve_price_fetch_range` / `index_ingestor._resolve_index_fetch_range`:

| Điều kiện | Khoảng fetch | Mode |
|---|---|---|
| `full_bootstrap_once_then_incremental=True` và chưa có marker `_full_bootstrap_done.json` | Full `years_back` | `bootstrap_full_once_*` |
| Có **watermark** và đã có file parquet tháng cho mã | `watermark + 1 ngày` → hôm nay | `incremental_watermark` |
| Đã có file, **chưa** watermark | `today - incremental_window_days` → hôm nay | `incremental_{N}d` |
| Chưa có file, `bootstrap_full_history_if_missing=True` | Full `years_back` | `bootstrap_full_*` |
| Chưa có file, bootstrap tắt | Cửa sổ `incremental_window_days` | `bootstrap_incremental_*` |

**Watermark đọc khi ingest** (`resolve_trading_date_watermark`): lấy **max** của:

1. Gold DB: `max(trading_date)` từ `gold.fact_price` / `gold.mart_stock_daily` (price) nếu có `DATABASE_URL`
2. Silver: `max(trading_date)` từ partition `data-lake/silver/price/` hoặc `index_price/`
3. Raw: `data-lake/raw/Structure_Data/_watermark.json` — key `price` / `index`

Ingestion **chỉ đọc** watermark; **ghi** sau Silver thành công (xem §5.4).

Nếu `start > end` (đã bắt kịp đến hôm nay), bỏ qua mã đó.

QC incremental: ngưỡng dòng thấp hơn (`min_ohlcv_rows_*_incremental`, mặc định 5).

#### C. Tùy chọn `full_bootstrap_once_then_incremental`

- Lần đầu (chưa có `price/_full_bootstrap_done.json` hoặc `index/_full_bootstrap_done.json`): full history một lần, sau run ghi marker JSON.
- Các lần sau: hành vi incremental như mục B.

### 5.2. `listing`

- Mỗi lần chạy: fetch toàn bộ universe → **ghi đè** `listing/master/listing.parquet`.
- Thêm cột metadata Bronze: `crawled_at` (= `run_date`), `source`.
- Silver chịu lọc cổ phiếu, chuẩn hóa symbol, dedupe analytics-ready.

### 5.3. `company`

- Snapshot theo ngày chạy: `company/snapshots/snapshot_date=<run_date>/company_overview.parquet`.
- Cùng `snapshot_date` trong một ngày ISO → **ghi đè** file (notebook dùng timestamp `run_partition` nên mỗi lần chạy Jupyter thường là partition khác).
- Mỗi mã: thử nguồn theo thứ tự KBS ưu tiên; method `overview` rồi `profile`; cột `ticker`, `source`, `company_method`, `snapshot_date`, `fetched_at`.

### 5.4. `financial_ratio`

- Mặc định **tắt** trong `run_structure_ingestion_pipeline`; bật qua full pipeline hoặc pipeline riêng.
- Mỗi mã: thử `ratio(period="quarter")` rồi `"year"` trên từng nguồn; lưu **một** dataframe đầu tiên không rỗng.
- Path: `financial_ratio/snapshot_date=<run_date>/<TICKER>.parquet` — ghi đè trong cùng snapshot.
- Có cơ chế disable nguồn lỗi tạm thời và dừng sớm sau N mã lỗi liên tiếp (`financial_ratio_abort_after_consecutive_source_errors`).

### 5.5. `price_board`

- Một request batch cho danh sách mã.
- Mỗi lần chạy: partition mới `snapshot_at=<YYYY-MM-DDTHH-MM-SS>` (UTC).
- File: `PRICE_BOARD_SNAPSHOT.parquet` (+ cột `snapshot_at`, `data_source`).

### 5.6. Watermark `_watermark.json` (OHLCV)

| Giai đoạn | Hành vi |
|---|---|
| **Ingest (Bronze)** | Đọc để tính `start` incremental; không bắt buộc phải có sẵn |
| **Silver CLI** | Sau `run_price_silver` / `run_index_price_silver` thành công → `write_raw_watermark` với `last_trading_date` = max ngày giao dịch đã ghi Silver |

Ví dụ nội dung:

```json
{
  "price": {
    "last_trading_date": "2026-05-18",
    "max_trading_date": "2026-05-18",
    "run_id": "2026-05-18",
    "updated_at_utc": "2026-05-18T08:55:53.526987+00:00"
  },
  "index": { "...": "..." }
}
```

Ghi watermark: **monotonic** (không lùi so với giá trị cũ).

### 5.7. Audit run OHLCV

Sau pipeline (hoặc khi gọi `write_run_metadata`):  
`price/_runs/<run_id>.json`, `index/_runs/<run_id>.json`

Payload gồm: `run_type` (`backfill` | `incremental` suy từ span ngày), `tickers`, `trading_date_from/to`, `row_count`, `status`.

---

## 6. Output Bronze

**Root:** `data-lake/raw/Structure_Data/` (tương đối repo root; gitignore)

```text
data-lake/raw/Structure_Data/
├── price/
│   ├── year=<YYYY>/month=<MM>/<TICKER>.parquet
│   └── _runs/<run_id>.json
├── index/
│   ├── year=<YYYY>/month=<MM>/<INDEX>.parquet
│   └── _runs/<run_id>.json
├── listing/master/listing.parquet
├── company/snapshots/snapshot_date=<run_date>/company_overview.parquet
├── financial_ratio/snapshot_date=<run_date>/<TICKER>.parquet
├── price_board/snapshot_at=<datetime>/PRICE_BOARD_SNAPSHOT.parquet
├── price/_full_bootstrap_done.json          # optional marker
├── index/_full_bootstrap_done.json          # optional marker
└── _watermark.json                          # cập nhật bởi Silver CLI
```

### 6.1. Metadata cột điển hình (Bronze)

**OHLCV (price/index)** — từ API + `build_price_like_schema`:

- Cột nguồn: `time` / `tradingdate` / `date`, `open`, `high`, `low`, `close`, `volume`, …
- Thêm: `ticker`, `trading_date` (khi ghi partition), `ingested_at` (= `run_date`), `fetched_at` (UTC ISO), `source`, `instrument_type` (`stock` | `index`)
- Chuẩn hóa analytics (dtype, `value` derived, …) do **Silver**, không Bronze.

**Listing / company / financial_ratio / price_board:** giữ dạng “raw-ish” từ vnstock + vài cột ingest; Silver transform chuẩn hóa.

### 6.2. Sơ đồ luồng

```mermaid
flowchart LR
  subgraph sources [Nguồn]
    VN[vnstock API]
    ENV[.env VNSTOCK_API_KEY]
  end

  subgraph ingest [Bronze ingestion]
    NB[ingest_structure_data_manager.ipynb]
    MOD[ingestion/structure_data/*]
    NB --> MOD
    ENV --> MOD
    VN --> MOD
  end

  subgraph bronze [data-lake/raw/Structure_Data]
    P[price/year/month]
    I[index/year/month]
    L[listing/master]
    C[company/snapshots]
    FR[financial_ratio/snapshots]
    PB[price_board/snapshot_at]
    WM[_watermark.json read]
  end

  subgraph silver [pipeline/silver]
    S[Silver parquet]
    WMU[write _watermark.json]
  end

  subgraph warehouse [warehouse/loader]
    PGs[(PostgreSQL silver.*)]
  end

  subgraph dbt [transform/dbt]
    G[(PostgreSQL gold.*)]
  end

  subgraph api [backend FastAPI]
    FE[React dashboard]
  end

  MOD --> bronze
  bronze --> S
  S --> WMU
  WMU --> WM
  WM -.->|incremental start| MOD
  S --> PGs
  PGs --> G
  G --> FE
```

---

## 7. Silver — Bronze → parquet sạch (`pipeline/silver/`)

### 7.1. Cấu trúc module

```text
pipeline/silver/
├── cli.py                          # CLI: --dataset all|price|index_price|...
├── config.py                       # SilverConfig (đường dẫn data-lake)
├── bronze_reader.py                # Đọc Bronze partition, BronzeBatch, ghi PART-000
├── price_transformer.py            # price + index_price (OHLCV)
├── structure_transformer.py        # listing + company
├── financial_ratio_transformer.py  # melt wide → long, partition period_type/year
├── price_board_transformer.py      # snapshot → trading_date, dedupe intraday
└── runs_log.py                     # Append audit _runs.jsonl
```

**Chạy Silver (6 dataset structured):**

```powershell
python -m pipeline.silver.cli --dataset all
python -m pipeline.silver.cli --dataset price
python -m pipeline.silver.cli --dataset index_price
python -m pipeline.silver.cli --dataset listing
python -m pipeline.silver.cli --dataset company
python -m pipeline.silver.cli --dataset financial_ratio
python -m pipeline.silver.cli --dataset price_board
```

Thứ tự khi `--dataset all` (trong `STRUCTURED_DATASETS`):

```text
price → index_price → listing → company → financial_ratio → price_board
```

Lỗi một dataset không chặn các dataset khác (log `FAILED`, exit code 1 cuối cùng).

### 7.2. Cơ chế đọc Bronze (`bronze_reader.py`)

| Dataset | Cách đọc Bronze | Incremental Silver |
|---|---|---|
| **price**, **index_price** | `read_partitioned_parquet` trên `price/` hoặc `index/` với `partition_key=month` (`year=*/month=*/*.parquet`) | Nếu không truyền `--run-partition`: watermark = max(`silver` partition `trading_date`, Gold `fact_price`/`mart_stock_daily` cho price) → chỉ đọc tháng Bronze `>=` tháng watermark và lọc dòng `trading_date > watermark` |
| **listing** | `read_master_parquet` — file master duy nhất | Luôn xử lý snapshot mới nhất, ghi đè Silver `current/` |
| **company** | `resolve_latest_snapshot_path` — snapshot `snapshot_date=*` mới nhất | Luôn lấy snapshot Bronze mới nhất, ghi đè Silver `current/` |
| **financial_ratio** | Glob `snapshot_date=*/*.parquet`, lọc `snapshot_date > watermark` Silver | Watermark = max `snapshot_date` đã có trong Silver (so sánh **chuỗi đầy đủ**, kể cả token `YYYY-MM-DDTHHMMSS`) |
| **price_board** | Glob `snapshot_at=*/PRICE_BOARD_SNAPSHOT.parquet` | Watermark = max `trading_date` partition Silver; chọn Bronze `snapshot_at.date >= watermark` |

Tùy chọn `--run-partition` / `--price-run-partition` / `--index-run-partition`: ép xử lý đúng một partition Bronze (tháng `YYYY-MM` cho OHLCV).

Mỗi batch Bronze được gắn `source_file`, `run_partition` (tháng hoặc snapshot) để trace.

### 7.3. Transform theo dataset

#### `price` / `index_price` (`price_transformer.py`)

- Chuẩn hóa cột OHLCV (`trading_date`, `ticker` / `index_code`, numeric, `volume` Int64).
- Derive `value` = `close * volume` nếu thiếu; `value_is_derived`, `is_suspicious` (high&lt;low, giá âm).
- Dedupe grain: **`ticker + trading_date`** (stock) hoặc **`index_code + trading_date`** (index); `keep="last"` theo `fetched_at`.
- Ghi Silver: **một file / ngày giao dịch** `silver/<dataset>/trading_date=<YYYY-MM-DD>/PART-000.parquet`.
- Lỗi DQ blocking → raise (thiếu cột bắt buộc); cảnh báo → log, vẫn ghi.

#### `listing` (`structure_transformer.py`)

- Lọc **chỉ cổ phiếu** (`security_type == stock` hoặc `id == 1`).
- Chuẩn hóa `symbol`, `exchange` (HOSE/HNX/UPCOM/UNKNOWN).
- Dedupe grain: **`symbol`**.
- Ghi: `silver/listing/current/PART-000.parquet` (ghi đè partition `current`).

#### `company` (`structure_transformer.py`)

- Chuẩn hóa text, parse ngày (`founded_date`, `listing_date`), số.
- Dedupe grain: **`ticker`** (giữ bản `snapshot_date` / `fetched_at` mới nhất).
- Ghi: `silver/company/current/PART-000.parquet`.

#### `financial_ratio` (`financial_ratio_transformer.py`)

- **Melt** Bronze wide (cột dạng `2024-Q1`, `2025-year`) → long: `ticker`, `item_code`, `item_name`, `period`, `period_type`, `year`, `quarter`, `value`.
- Dedupe grain: **`ticker + item_code + period`**; merge với partition Silver cũ rồi ghi đè `PART-000`.
- Ghi: `silver/financial_ratio/period_type=<quarter|annual>/year=<YYYY>/PART-000.parquet`.

#### `price_board` (`price_board_transformer.py`)

- Map cột giá/khối lượng; `trading_date` = ngày của `snapshot_at`.
- Dedupe grain: **`symbol + trading_date`** — giữ snapshot **mới nhất** trong ngày (`dedupe_to_daily_latest`).
- Ghi: `silver/price_board/trading_date=<YYYY-MM-DD>/PART-000.parquet`.

### 7.4. Grain & quy tắc đúng đắn (Silver)

| Dataset Silver | Grain (khóa logic) | Ghi chú |
|---|---|---|
| `price` | `ticker + trading_date` | Partition theo `trading_date` |
| `index_price` | `index_code + trading_date` | Cột symbol đổi tên từ `ticker` |
| `listing` | `symbol` | Snapshot `current` |
| `company` | `ticker` | Snapshot `current` |
| `financial_ratio` | `ticker + item_code + period` | Watermark snapshot full token |
| `price_board` | `symbol + trading_date` | Latest `snapshot_at` trong ngày |

### 7.5. Output Silver

```text
data-lake/silver/
├── price/trading_date=<YYYY-MM-DD>/PART-000.parquet
├── index_price/trading_date=<YYYY-MM-DD>/PART-000.parquet
├── listing/current/PART-000.parquet
├── company/current/PART-000.parquet
├── financial_ratio/period_type=<quarter|annual>/year=<YYYY>/PART-000.parquet
├── price_board/trading_date=<YYYY-MM-DD>/PART-000.parquet
└── <dataset>/_runs.jsonl                    # audit JSON Lines mỗi lần transform
```

Mỗi dòng `_runs.jsonl`: `run_id`, `dataset`, `run_partition`, `started_at`, `finished_at`, `input_rows`, `output_rows`, `latest_key`, `status`, `error`.

### 7.6. Cập nhật watermark Raw (OHLCV)

Sau `run_price_silver` / `run_index_price_silver` thành công, `cli.py` gọi `write_raw_watermark` trên `data-lake/raw/Structure_Data/_watermark.json`:

- Key raw: `price` ← dataset Silver `price`; `index` ← Silver `index_price`.
- `last_trading_date` = max ngày trong các partition Silver vừa ghi.
- Không ghi nếu không có partition `trading_date` mới.

---

## 8. Warehouse — Silver → PostgreSQL (`warehouse/loader/`)

### 8.1. CLI

```powershell
$env:DATABASE_URL = "postgresql://stock:stock@localhost:55432/stock_pipeline"
python -m warehouse.loader.cli load-silver --dataset all
python -m warehouse.loader.cli load-silver --dataset price,index_price,listing
```

Thứ tự khi `all` (structured trước news/BCTC):

```text
price → index_price → listing → company → financial_ratio → price_board → news → bctc_pdf_meta
```

Cần DDL đã apply: `warehouse/ddl/schema.sql` (schema `silver`, bảng + PK/unique cho upsert).

### 8.2. Cơ chế load (`silver_loader.py`)

1. **Đọc:** `glob` recursive toàn bộ parquet Silver của dataset (ví dụ `data-lake/silver/price/**/*.parquet`).
2. **Chuẩn hóa:** `prepare_dataframe` — ép dtype (date, timestamptz, text), chỉ giữ cột whitelist, map NULL Python-safe.
3. **Validate:** khóa không null, không trùng trên `key_cols` (blocking trước khi ghi DB).
4. **Upsert:** `INSERT ... ON CONFLICT (key_cols) DO UPDATE` theo batch (mặc định 2000 dòng); đếm insert vs update qua `RETURNING (xmax = 0)`.
5. **Audit:** ghi `silver.load_audit` (`dataset`, `run_partition`, `rows_read`, `rows_inserted`, `rows_updated`, `status`, `error_msg`).

Load **idempotent**: chạy lại cùng Silver parquet → update các dòng trùng khóa, không nhân đôi.

### 8.3. Bảng PostgreSQL & khóa upsert (structured)

| Dataset | Bảng | Upsert key |
|---|---|---|
| `price` | `silver.price` | `ticker`, `trading_date` |
| `index_price` | `silver.index_price` | `index_code`, `trading_date` |
| `listing` | `silver.listing` | `symbol` |
| `company` | `silver.company` | `ticker` |
| `financial_ratio` | `silver.financial_ratio` | `ticker`, `item_code`, `period` |
| `price_board` | `silver.price_board` | `symbol`, `trading_date` |

Loader đọc **toàn bộ** file parquet khớp glob mỗi lần chạy (full refresh từ filesystem), không incremental riêng ở tầng warehouse — incremental nằm ở Bronze ingest và Silver transform.

---

## 9. Gold — Silver → analytics (`transform/dbt/`)

dbt đọc **bảng PostgreSQL schema `silver`** (đã load từ parquet), build model trong **schema `gold`** (`profiles.yml`: `schema: gold`). Launcher chạy từ repo root: `dbt_project.yml` trỏ `model-paths` → `transform/dbt/models`.

### 9.1. Cấu trúc project & lệnh chạy

```text
transform/dbt/
├── dbt_project.yml       # materialized: staging=view, intermediate/marts=table
├── profiles.yml          # Postgres localhost:55432, schema gold
└── models/
    ├── staging/          # stg_* — passthrough + cast từ silver.*
    ├── intermediate/     # int_price_indicator (structured); int_news_* (news)
    └── marts/            # fact_*, dim_*, mart_*
```

```powershell
dbt debug --profiles-dir transform/dbt
dbt run --profiles-dir transform/dbt
dbt test --profiles-dir transform/dbt
```

`dbt run` build **toàn bộ** DAG (gồm cả model news/BCTC nếu `silver.news` / `silver.bctc_pdf_meta` đã có dữ liệu). Không có incremental model riêng — mỗi lần `run` refresh table theo cấu hình materialization.

### 9.2. Giải thích mô hình bảng Gold — `stg`, `fact`, `dim`, `mart`

**Gold** là lớp analytics “sẵn sàng dùng”: dbt đọc `silver.*`, tính toán thêm, ghi ra schema **`gold`**. API/frontend **không** đọc Silver — chỉ đọc Gold (và trường hợp đặc biệt `gold.stg_financial_ratio` cho bảng chỉ số chi tiết).

Không phải mọi object trong `gold` đều là **mart**. Trong luồng structured, có **bốn vai trò**:

```text
silver.*  →  stg_* (view)       chuẩn hóa / cast, gần nguồn
         →  int_* (table)       logic trung gian (VD: chỉ báo kỹ thuật)
         →  fact_* / dim_*      sự kiện theo thời gian / danh mục thực thể
         →  mart_* (table)      bảng tối ưu cho một màn hình hoặc endpoint API
```

#### Vì sao chia nhiều lớp thay vì một bảng Gold lớn?

| Lý do | Giải thích |
|---|---|
| Tái sử dụng | `fact_price_daily` là nguồn chung cho `mart_stock_daily`, `mart_market_overview`, `mart_company_profile` — tránh copy cùng logic OHLCV + indicator ở nhiều nơi |
| Tách trách nhiệm | Silver giữ grain ingest; Gold thêm MA/RSI/MACD, pivot chỉ số, JSON top movers |
| API đơn giản | Mỗi endpoint thường `SELECT` **một mart** (hoặc một view staging), không join 5 bảng mỗi request |
| Kimball / dbt convention | `fact` + `dim` là pattern chuẩn; `mart` là lớp “product” phía trên |

#### `stg_*` — Staging (view)

- Ví dụ: `gold.stg_price`, `gold.stg_financial_ratio`.
- **Không phải mart** — chỉ là lớp đọc từ `silver` + ép kiểu / đổi tên cột.
- Dùng khi cần **chi tiết đầy đủ** (mọi `item_code` × `period` của financial ratio) → API `GET /financials/{symbol}`.

#### `int_*` — Intermediate (table)

- Ví dụ: `gold.int_price_indicator`.
- Grain vẫn `ticker + trading_date`; chứa logic **nặng** (rolling window trên toàn lịch sử).
- `fact_price_daily` **join** vào đây thay vì nhét indicator vào Silver.

#### `fact_*` — Fact (sự kiện theo thời gian)

| Bảng | Grain | Ý nghĩa |
|---|---|---|
| `fact_price_daily` | `ticker + trading_date` | Mỗi ngày giao dịch một mã: OHLCV + indicator |
| `fact_index_daily` | `index_code + trading_date` | OHLCV chỉ số (VNINDEX, VN30, …) |

**Fact** trả lời: *điều gì xảy ra **khi nào**?* API **không** expose fact trực tiếp; mart và SQL overview đọc từ fact.

#### `dim_*` — Dimension (danh mục / mô tả)

| Bảng | Grain | Ý nghĩa |
|---|---|---|
| `dim_security` | `symbol` | Universe mã từ listing |
| `dim_company` | `ticker` | Hồ sơ công ty (+ tên từ listing) |

**Dimension** trả lời: *thực thể **là ai / là gì**?* Ít thay đổi theo từng ngày giao dịch (snapshot `current` ở Silver).

#### `mart_*` — Mart là gì?

**Mart** (data mart) = bảng **phục vụ một use case cụ thể** — thường trùng với một **màn hình** hoặc **một API**.

| Đặc điểm | Mart trong dự án này |
|---|---|
| Grain | Khớp cách UI/API đọc (1 dòng/ngày thị trường, 1 dòng/mã profile, 1 dòng/mã/ngày giá) |
| Nội dung | Đã join, aggregate, pivot sẵn |
| Materialization | **Table** trong PostgreSQL (đọc nhanh cho dashboard) |

| Mart | Grain | Khác fact/dim thế nào? | API / UI |
|---|---|---|---|
| **`mart_stock_daily`** | `ticker + trading_date` | = `fact_price_daily` + (tuỳ chọn) sentiment tin theo ngày | `/prices`, `/indicators` — biểu đồ |
| **`mart_company_profile`** | `ticker` (1 dòng/mã) | Gộp `dim_company` + giá mới nhất + 52w + PE/PB/… | `/companies/{symbol}` |
| **`mart_market_overview`** | `trading_date` (1 dòng/ngày) | Pivot chỉ số + breadth + top movers JSON | `/market/overview` |
| **`mart_ticker_directory`** | `ticker` | Union mã từ price, company, news, BCTC + cờ `has_*` | **`GET /tickers`** |

So sánh ngắn:

```text
fact_price_daily      → lịch sử giá + indicator (nhiều dòng / mã)
mart_stock_daily      → cùng nguồn, grain API chart (+ news nếu có)

dim_company           → hồ sơ tĩnh
mart_company_profile  → hồ sơ + snapshot metrics kinh doanh → 1 request / 1 mã
```

#### Trường hợp đặc biệt trong luồng structured

| Object | Ghi chú |
|---|---|
| `stg_financial_ratio` | Vừa feed API chi tiết (`/financials`), vừa pivot vào `mart_company_profile` (PE, PB, …) |
| `stg_price_board` | Có view Gold, **chưa** có fact/mart → **chưa có UI/API** |
| `int_news_sentiment_daily` | Intermediate luồng news; join vào `mart_stock_daily` (không phải mart structured thuần) |

Chi tiết từng model (SQL, cột): các mục **§9.4–§9.8** bên dưới. Mapping API/UI: **§10**.

### 9.3. Sources (`models/staging/sources.yml`)

dbt khai báo `source('silver', …)` cho 6 bảng structured:

| Source | Bảng Silver |
|---|---|
| `silver.price` | OHLCV cổ phiếu |
| `silver.index_price` | OHLCV chỉ số |
| `silver.listing` | Universe mã |
| `silver.company` | Hồ sơ công ty |
| `silver.financial_ratio` | Chỉ số tài chính (long) |
| `silver.price_board` | Bảng giá snapshot |

### 9.4. Staging (`stg_*`) — materialized **view** trong `gold`

| Model | Nguồn | Xử lý chính |
|---|---|---|
| `stg_price` | `silver.price` | Cast numeric; giữ metadata lineage |
| `stg_index_price` | `silver.index_price` | Tương tự OHLCV |
| `stg_listing` | `silver.listing` | Passthrough listing |
| `stg_company` | `silver.company` | Map alias (`company_name`, `industry` từ `company_type`, …) |
| `stg_financial_ratio` | `silver.financial_ratio` | Cast `value` double precision |
| `stg_price_board` | `silver.price_board` | Cast giá/khối lượng |

**Lưu ý:** `stg_price_board` **không** được ref bởi mart/fact nào — chỉ tồn tại view `gold.stg_price_board` phục vụ ad-hoc/SQL. API hiện **không** expose price board.

`stg_financial_ratio` được dùng trực tiếp bởi API (`GET /financials/{symbol}` → `gold.stg_financial_ratio`) và gián tiếp trong `mart_company_profile` (PE/PB/EPS/ROE/ROA).

### 9.5. Intermediate (structured)

#### `int_price_indicator` (table `gold.int_price_indicator`)

- Input: `stg_price`.
- Tính theo `ticker`, sort `trading_date`:
  - `daily_return` = % thay đổi `close`.
  - **MA7 / MA20 / MA50** — SMA rolling (NULL cho đến khi đủ cửa sổ).
  - **RSI14** — Cutler SMA gain/loss (không phải Wilder EMA).
  - **MACD** — `SMA12 - SMA26`, `macd_signal` = SMA9 của `macd_line`, `macd_hist`.
  - **Bollinger** — middle = MA20, upper/lower = ±2× std20.
- Grain: `ticker + trading_date`.

### 9.6. Facts & dimensions (structured)

| Model | Materialization | Grain | Mô tả |
|---|---|---|---|
| `fact_price_daily` | table | `ticker + trading_date` | `stg_price` LEFT JOIN `int_price_indicator` — OHLCV + toàn bộ indicator |
| `fact_index_daily` | table | `index_code + trading_date` | `stg_index_price` (OHLCV rút gọn) |
| `dim_security` | table | `symbol` | `stg_listing` — symbol, tên, sàn, loại |
| `dim_company` | table | `ticker` | `stg_company` LEFT JOIN `stg_listing` (bổ sung `organ_name`) |

### 9.7. Marts (output analytics chính — structured)

#### `mart_stock_daily` (table `gold.mart_stock_daily`)

- `fact_price_daily` LEFT JOIN `int_news_sentiment_daily` trên `ticker` + `trading_date = published_date`.
- Cột giá + indicator từ fact; thêm `news_count`, `avg_sentiment_score`, `dominant_sentiment` (0 nếu không có tin).
- **Phụ thuộc luồng news:** cần `silver.news` + `int_news_sentiment_daily` để cột sentiment đầy đủ; nếu chưa ingest news, join vẫn chạy, sentiment NULL/0.
- Grain: `ticker + trading_date` (Timescale hypertable theo DDL nếu đã apply).
- Snapshot demo: **62,339** rows.

#### `mart_company_profile` (table `gold.mart_company_profile`)

- `dim_company` + metrics từ `fact_price_daily`:
  - `latest_close`, `latest_trading_date`
  - `high_52w` / `low_52w` (365 ngày gần max trading date)
  - `avg_volume_20d`
- Ratios từ `stg_financial_ratio`: lấy **period mới nhất** có `item_code` ∈ (`pe_ratio`, `pb_ratio`, `trailing_eps`, `roe`, `roa`) → pivot `pe_ratio`, `pb_ratio`, `eps`, `roe`, `roa`.
- Grain: **1 dòng / ticker**. Snapshot demo: **50** rows.

#### `mart_market_overview` (table `gold.mart_market_overview`)

- **Chỉ số:** `fact_index_daily` → return ngày; pivot `VNINDEX`, `VN30`, `HNXINDEX` (close + return).
- **Thị trường cổ phiếu:** aggregate `fact_price_daily` theo ngày — `total_volume`, `total_value`, đếm `advances` / `declines` / `unchanged`.
- **Top movers:** JSON `top_gainers` / `top_losers` (top 5 theo `daily_return` mỗi ngày).
- Grain: **1 dòng / trading_date**. Snapshot demo: **1,247** rows.

### 9.8. DAG phụ thuộc (structured + liên kết news)

```mermaid
flowchart TB
  subgraph silver_pg [PostgreSQL silver]
    sp[price]
    si[index_price]
    sl[listing]
    sc[company]
    sfr[financial_ratio]
    spb[price_board]
    sn[news]
  end

  subgraph stg [gold staging views]
    stg_p[stg_price]
    stg_i[stg_index_price]
    stg_l[stg_listing]
    stg_c[stg_company]
    stg_fr[stg_financial_ratio]
    stg_pb[stg_price_board]
  end

  sp --> stg_p
  si --> stg_i
  sl --> stg_l
  sc --> stg_c
  sfr --> stg_fr
  spb --> stg_pb

  stg_p --> int_pi[int_price_indicator]
  stg_p --> fpd[fact_price_daily]
  int_pi --> fpd
  stg_i --> fid[fact_index_daily]

  stg_l --> ds[dim_security]
  stg_c --> dc[dim_company]
  stg_l --> dc

  fpd --> msd[mart_stock_daily]
  sn --> int_news[int_news_sentiment_daily]
  int_news --> msd

  fpd --> mcp[mart_company_profile]
  dc --> mcp
  stg_fr --> mcp

  fid --> mmo[mart_market_overview]
  fpd --> mmo

  stg_fr --> api_fin["API /financials"]
  msd --> api_px["API /prices /indicators"]
  mcp --> api_co["API /companies"]
  mtd[mart_ticker_directory]
  fpd --> mtd
  mcp --> mtd
  mtd --> api_tick["API /tickers"]
  mmo --> api_mkt["API /market/overview"]
```

Luồng **news** / **BCTC** cũng feed `mart_ticker_directory` — chi tiết trong tài liệu riêng.

### 9.9. Output cuối cùng — bảng Gold & API

Tất cả relation vật lý nằm schema **`gold`** (view staging + table intermediate/marts). Giải thích mart/fact/dim: **§9.2**. Chi tiết màn hình UI: **§10**.

| Đối tượng Gold | Loại | Grain | API / UI |
|---|---|---|---|
| `stg_price`, `stg_index_price`, … | view | — | Không gọi trực tiếp |
| `stg_financial_ratio` | view | `ticker + item_code + period` | `GET /financials/{symbol}` |
| `int_price_indicator` | table | `ticker + trading_date` | Qua `mart_stock_daily` / `fact_price_daily` |
| `fact_price_daily` | table | `ticker + trading_date` | Nền cho marts (không expose API) |
| `fact_index_daily` | table | `index_code + trading_date` | Qua `mart_market_overview` |
| `dim_security` | table | `symbol` | Nền dims / tests |
| `dim_company` | table | `ticker` | Nền `mart_company_profile` |
| `fact_news_article` | table | `article_id` | Xem [News_data_flow.md](News_data_flow.md) — `/news/.../articles` |
| **`mart_stock_daily`** | table | `ticker + trading_date` | `GET /prices/{symbol}`, `GET /indicators/{symbol}` |
| **`mart_company_profile`** | table | `ticker` | `GET /companies/{symbol}` |
| **`mart_market_overview`** | table | `trading_date` | `GET /market/overview` |
| **`mart_ticker_directory`** | table | `ticker` | **`GET /tickers`** (search universe + `has_news`, `has_bctc`) |

**Chưa có mart Gold cho:** `price_board` (chỉ `silver.price_board` + `gold.stg_price_board`).

**dbt tests** (`schema.yml`): `not_null`, `unique`, `unique_combination_of_columns` trên grain staging/marts; `mart_stock_daily.ticker` có `relationships` → `dim_security.symbol`.

### 9.10. Snapshot row counts (demo local, 2026-06-01)

| Layer | Object | Rows (demo) |
|---|---|---:|
| Silver PG | `silver.price` | 62,339 |
| Silver PG | `silver.index_price` | 6,234 |
| Silver PG | `silver.listing` | 1,535 |
| Silver PG | `silver.company` | 50 |
| Silver PG | `silver.financial_ratio` | 15,282 |
| Silver PG | `silver.price_board` | 50 |
| Gold | `mart_stock_daily` | 62,339 |
| Gold | `mart_company_profile` | 50 |
| Gold | `mart_market_overview` | 1,247 |

---

## 10. Ứng dụng — Backend API & Frontend UI

Tầng cuối của luồng structured data: **FastAPI** đọc schema `gold`, **React/Vite** gọi API và render dashboard. Chỉ **GET**, không ghi DB.

### 10.1. Kiến trúc Backend (`backend/`)

```text
backend/
├── main.py              # FastAPI app, CORS, mount routers
├── config.py            # DATABASE_URL, pagination (page_size 100, max 500)
├── database.py          # psycopg2 connection pool (2–10 conn)
├── dependencies.py      # get_db — yield connection / rollback
├── routers/             # HTTP handlers (SQL trực tiếp → gold.*)
│   ├── tickers.py       # structured
│   ├── market.py
│   ├── companies.py
│   ├── prices.py
│   ├── indicators.py
│   ├── financials.py
│   ├── news.py            # luồng tin — xem News_data_flow.md
│   ├── bctc.py            # luồng BCTC — xem BCTC_data_flow.md
│   └── health.py
└── schemas/             # Pydantic response models
```

| Đặc điểm | Chi tiết |
|---|---|
| Kết nối | `DATABASE_URL` (mặc định `postgresql://stock:stock@localhost:55432/stock_pipeline`) |
| Schema đọc | Chỉ `gold.*` — không truy vấn `silver` hay file |
| CORS | `localhost:5173`, `localhost:3000`; methods **GET** |
| Phân trang | `page`, `page_size` (mặc định 100, tối đa 500) trên `/prices`, `/indicators` |
| Swagger | `http://localhost:8000/docs` |

**Chạy API:**

```powershell
$env:DATABASE_URL = "postgresql://stock:stock@localhost:55432/stock_pipeline"
uvicorn backend.main:app --reload --port 8000
```

### 10.2. API structured — endpoint → Gold → response

Các endpoint dưới đây map trực tiếp tới pipeline structured (price, index, listing, company, financial_ratio). Router **news** / **bctc** phục vụ luồng khác, không liệt kê chi tiết ở đây.

| Method | Endpoint | Bảng Gold | Response schema | Query |
|---|---|---|---|---|
| GET | `/tickers` | **`mart_ticker_directory`** | `TickerListResponse` (`has_price`, `has_news`, `has_bctc`, …) | — |
| GET | `/market/overview` | `mart_market_overview` | `MarketOverviewResponse` | `date` (optional) |
| GET | `/companies/{symbol}` | `mart_company_profile` | `CompanyProfileResponse` | — |
| GET | `/prices/{symbol}` | `mart_stock_daily` | `PaginatedResponse[PriceRow]` | `from`, `to`, `page`, `page_size` |
| GET | `/indicators/{symbol}` | `mart_stock_daily` (+ tính thêm) | `PaginatedResponse[IndicatorRow]` | `from`, `to`, `page`, `page_size` |
| GET | `/financials/{symbol}` | `stg_financial_ratio` | `list[FinancialRatioRow]` | `period_type` = `quarter` \| `annual` |

**Cột trả về chính (structured):**

- **PriceRow:** `ticker`, `trading_date`, OHLCV, `value`, `daily_return`
- **IndicatorRow:** `close`, MA7/20/50, RSI14, MACD line/signal/hist, Bollinger bands, **`volatility_20d`** (std 20 ngày của `daily_return` — tính trong SQL API, không có sẵn trong mart)
- **CompanyProfileResponse:** hồ sơ + `latest_close`, 52w high/low, `avg_volume_20d`, PE/PB/EPS/ROE/ROA
- **MarketOverviewResponse:** VNINDEX/VN30/HNX close & return, breadth, `top_gainers` / `top_losers` (JSON array)
- **FinancialRatioRow:** `period`, `item_code`, `item_name`, `value`, …

**Không expose qua API (dù đã ingest):**

| Dữ liệu | Lý do |
|---|---|
| `silver.price_board` / `gold.stg_price_board` | Chưa có router hay màn hình |
| `fact_price_daily`, `fact_index_daily` | Nội bộ dbt; UI dùng `mart_*` |
| `dim_company` trực tiếp | Gộp trong `mart_company_profile` |

Lỗi **404:** API trả HTTP 404; frontend axios interceptor coi 404 là `data: null` (empty state, không crash).

### 10.3. Kiến trúc Frontend (`frontend/`)

```text
frontend/src/
├── api/
│   ├── client.ts          # Axios, baseURL VITE_API_URL hoặc /api
│   ├── stocks.ts          # tickers, company, prices, indicators, financials
│   └── market.ts          # market overview
├── hooks/                 # TanStack Query wrappers
├── pages/
│   ├── DashboardPage.tsx  # Tổng quan thị trường
│   └── StockDetailPage.tsx
├── components/
│   ├── market/            # IndexCard, TopMoversTable, MarketOverviewSection
│   ├── stock/             # PriceChart, IndicatorChart, FinancialTable
│   └── shared/SearchBar.tsx
└── types/index.ts         # TypeScript mirrors Pydantic schemas
```

| Công nghệ | Vai trò |
|---|---|
| React 18 + Vite | SPA, lazy routes |
| TanStack Query | Cache API (`staleTime` 30s–5 phút) |
| Axios | HTTP; proxy dev `/api` → `localhost:8000` (`vite.config.ts`) |
| Recharts | Biểu đồ giá & chỉ báo |
| React Router | `/`, `/stock/:symbol` |
| Tailwind | UI dark theme |

**Chạy UI:**

```powershell
cd frontend
npm run dev
# http://localhost:5173  — API qua proxy /api
```

Biến `VITE_API_URL` (optional): nếu set thì gọi thẳng URL đó, không qua proxy.

### 10.4. Màn hình & output người dùng thấy (structured)

```mermaid
flowchart TB
  subgraph dash [Route / Dashboard]
    T[SearchBar - tickers list]
    I[IndexCard VNINDEX VN30 HNX]
    B[Breadth Tang Giam Dung KL]
    M[TopMoversTable gainers losers]
  end

  subgraph detail [Route /stock/:symbol]
    H[Header gia + ho so]
    O[Tab Tong quan - company profile]
    C[Tab Bieu do - PriceChart close]
    IND[Tab Chi bao - IndicatorChart]
    FIN[Tab Tai chinh - FinancialTable]
  end

  API_M[/market/overview] --> I
  API_M --> B
  API_M --> M
  API_T[/tickers] --> T
  API_C[/companies/symbol] --> H
  API_C --> O
  API_P[/prices/symbol] --> C
  API_I[/indicators/symbol] --> IND
  API_F[/financials/symbol] --> FIN
```

#### Trang Dashboard (`/`)

| UI block | Hook / API | Nguồn Gold | Nội dung hiển thị |
|---|---|---|---|
| Số ticker demo | `useTickers` → `GET /tickers` | `mart_company_profile` | Đếm universe (~50 mã demo) |
| **SearchBar** | `useTickers` | cùng trên | Gợi ý mã + tên công ty → navigate `/stock/{ticker}` |
| **3 IndexCard** | `useMarketOverview` → `GET /market/overview` | `mart_market_overview` (ngày mới nhất) | VN-Index, VN30, HNX: close + % ngày |
| **Breadth 4 ô** | `useMarketOverview` | `mart_market_overview` | Tăng / Giảm / Đứng giá / Khối lượng |
| **TopMoversTable** | `useMarketOverview` | `top_gainers`, `top_losers` JSON | Top 5 tăng/giảm: ticker, close, % — link sang trang mã |

#### Trang chi tiết mã (`/stock/:symbol`)

| Tab | Component | API | Gold | Output |
|---|---|---|---|---|
| *(header)* | `useCompany` | `GET /companies/{symbol}` | `mart_company_profile` | Ticker, sàn, ngành, tên, **giá đóng cửa mới nhất** |
| **Tổng quan** | `StockDetailPage` | `useCompany` | `mart_company_profile` | Hồ sơ (niên hiệu, ngày niêm yết, website, vốn…) + **Key metrics** (52w, vol 20d, P/E, P/B, EPS, ROE, ROA) + mô tả |
| **Biểu đồ** | `PriceChart` | `usePrices` (`page_size=500`) | `mart_stock_daily` | Line chart **close**; filter 1M/3M/6M/1Y/All |
| **Chỉ báo** | `IndicatorChart` | `useIndicators` (`page_size=500`) | `mart_stock_daily` | Toggle MA7/20/50, RSI14, MACD + đường close |
| **Tài chính** | `FinancialTable` | `useFinancials` | `stg_financial_ratio` | Bảng period × metric (quarter/annual/all); tối đa 160 dòng hiển thị |

Tab **Tin tức** và **BCTC** trên cùng trang dùng luồng news/PDF (không thuộc structured thuần); pipeline structured vẫn cấp **PE/PB/…** qua `mart_company_profile` và có thể cấp sentiment trên `mart_stock_daily` nếu đã chạy ingest news + dbt.

### 10.5. Luồng dữ liệu structured → mắt người dùng (tóm tắt)

| Bronze dataset | Gold object chính | API | Màn hình UI |
|---|---|---|---|
| `price` | `mart_stock_daily` | `/prices`, `/indicators` | Biểu đồ giá, chỉ báo kỹ thuật |
| `index` | `mart_market_overview` | `/market/overview` | Index cards, breadth (gián tiếp từ price agg) |
| `listing` + `company` | `mart_company_profile`, `dim_security` | `/companies`, `/tickers` | Header mã, tab Tổng quan, search |
| `financial_ratio` | `stg_financial_ratio` | `/financials` | Tab Tài chính; PE/PB trên Tổng quan |
| `price_board` | — | — | **Chưa có UI** |

**Điều kiện để UI đầy đủ:** PostgreSQL có dữ liệu sau `load-silver` + `dbt run`; API và frontend cùng `DATABASE_URL` / proxy. **Search ticker** đọc `mart_ticker_directory` (union mã có price/news/BCTC/profile — rộng hơn chỉ `mart_company_profile` ~50 mã demo). Toàn bộ `silver.listing` (~1500 mã) không tự hiện nếu chưa ingest giá/tin/BCTC cho mã đó.

---

## 11. Runbook end-to-end (structured)

```powershell
# 1. Bronze
@'
from ingestion.structure_data import IngestionConfig, run_structure_ingestion_pipeline
print(run_structure_ingestion_pipeline(IngestionConfig()))
'@ | python -

# 2. Silver parquet
python -m pipeline.silver.cli --dataset price,index_price,listing,company,financial_ratio,price_board

# 3. PostgreSQL + DDL
.\warehouse\scripts\setup_db.ps1

# 4. Load Silver → silver.*
$env:DATABASE_URL = "postgresql://stock:stock@localhost:55432/stock_pipeline"
python -m warehouse.loader.cli load-silver --dataset price,index_price,listing,company,financial_ratio,price_board

# 5. Gold (dbt) — cần silver.news nếu muốn sentiment trên mart_stock_daily
dbt run --profiles-dir transform/dbt
dbt test --profiles-dir transform/dbt

# 6. API + Frontend
uvicorn backend.main:app --reload --port 8000
cd frontend && npm run dev
# http://localhost:5173  |  Swagger http://localhost:8000/docs
```

Thứ tự bắt buộc: Bronze → Silver files → load `silver.*` → `dbt run` → API → UI. Chạy lại `dbt run` sau khi reload Silver là đủ cập nhật Gold; refresh trình duyệt để thấy dữ liệu mới.

---

## 12. Tham số `IngestionConfig` quan trọng (Bronze)

| Tham số | Mặc định | Ý nghĩa |
|---|---|---|
| `tickers` | 20 mã VN30 | Danh sách OHLCV / company / board / ratio |
| `index_tickers` | 5 chỉ số | OHLCV index |
| `primary_source` / `fallback_source` | `kbs` / `vci` | Thứ tự thử nguồn |
| `years_back` | 5 | Độ sâu backfill |
| `use_incremental_window` | `True` (code); notebook backfill: `False` | Bật/tắt logic incremental |
| `incremental_window_days` | 10 (code); daily profile: `1` | Cửa sổ khi có file nhưng chưa watermark |
| `bootstrap_full_history_if_missing` | `True` | Mã mới → full history |
| `full_bootstrap_once_then_incremental` | `False` | Full một lần + marker |
| `max_tickers_per_run` | 50 | Giới hạn mã mỗi run |
| `rate_limit_rpm` | 10 | Rate limit toàn cục |
| `delay_between_categories_sec` | 30 | Nghỉ giữa nhóm API trong pipeline |

---

## 13. Tài liệu liên quan

- [README.md](../README.md) — runbook tổng hợp, endpoints, snapshot
- [ingestion/ingest_structure_data_manager.ipynb](../ingestion/ingest_structure_data_manager.ipynb) — RUN_PROFILE, danh sách mã
- `ingestion/structure_data/config.py` — default Bronze config
- `pipeline/silver/cli.py` — Bronze → Silver
- `warehouse/loader/silver_loader.py` — Silver → PostgreSQL
- `transform/dbt/models/` — staging / intermediate / marts SQL
- `transform/dbt/profiles.yml` — kết nối DB, schema `gold`
- `warehouse/ddl/schema.sql` — DDL `silver` (+ một phần `gold` mẫu; dbt tạo/refresh bảng gold khi `dbt run`)
- `backend/routers/` — SQL từng endpoint
- `backend/schemas/` — contract JSON API
- `frontend/src/api/stocks.ts`, `frontend/src/api/market.ts` — client gọi API
- `frontend/src/pages/` — Dashboard & Stock detail
