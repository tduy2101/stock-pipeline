# Structure Data Ingestion

Tài liệu mô tả **luồng ingest dữ liệu có cấu trúc** hiện tại: từ cấu hình → gọi vnstock → QC → ghi `data-lake/raw/Structure_Data/`.

## Luồng tổng thể

```mermaid
flowchart LR
  subgraph prep [Chuẩn bị]
    ENV[".env VNSTOCK_API_KEY"]
    CFG["IngestionConfig"]
    ENV --> REG["register_vnstock_api_key_from_env"]
    REG --> CFG
  end
  subgraph pipe [Pipeline tuần tự]
    P["price OHLCV"]
    I["index OHLCV"]
    L["listing"]
    C["company overview"]
    F["financial_ratio"]
    B["price_board"]
    P --> I --> L --> C
    C --> FR{"include financial_ratio?"}
    FR -->|có| F
    FR -->|không mặc định| B
    F --> B
  end
  subgraph per_ohlcv [Mỗi bước OHLCV]
    R["resolve khoảng ngày"]
    Q["Quote.history + fallback nguồn"]
    T["transform_ohlcv + validate"]
    S["save_partition_parquet"]
    R --> Q --> T --> S
  end
  CFG --> pipe
  P -.-> per_ohlcv
  I -.-> per_ohlcv
```

1. **Cấu hình** (`IngestionConfig` trong `config.py`): danh sách mã, chỉ số, `primary_source` / `fallback_source`, rate limit, cửa sổ incremental, ngưỡng QC, đường dẫn data lake.
2. **API vnstock**: đăng ký key (`common.register_vnstock_api_key_from_env`), mỗi request qua `wait_for_rate_limit` và `call_with_retry` khi lỗi mạng tạm thời.
3. **Pipeline** (`pipeline.py`): gọi lần lượt các ingestor, **nghỉ** `delay_between_categories_sec` giữa các nhóm để giảm rate limit / lỗi mạng.
4. **Ghi dữ liệu**: Parquet dưới `data-lake/raw/Structure_Data/` (partition `date=<run_date>` hoặc thư mục `master` — xem bảng dưới).

## 1) Tổng quan file trong thư mục

| File | Vai trò |
|------|---------|
| `config.py` | `IngestionConfig`: tickers, chỉ số, nguồn, rate limit, backfill/incremental, QC, retry, `data_lake_root` |
| `common.py` | Rate limit, retry, chuẩn hóa OHLCV, QC số dòng, schema giá/chỉ số, lưu Parquet, nạp `.env`, đăng ký key vnstock |
| `price_ingestor.py` | OHLCV cổ phiếu (`Quote.history`) |
| `index_ingestor.py` | OHLCV chỉ số (cùng pattern với giá) |
| `stock_info_ingestor.py` | Listing, company overview/profile, financial ratio, price board |
| `pipeline.py` | `run_structure_ingestion_pipeline` và biến thể (có/không `financial_ratio`) |

## 2) Thứ tự chạy pipeline (`pipeline.py`)

Mặc định `run_structure_ingestion_pipeline`:

1. `ingest_prices` → pause  
2. `ingest_indices` → pause  
3. `ingest_listing` → pause  
4. `ingest_company_overview` → pause  
5. `ingest_financial_ratio` — **mặc định tắt** (`include_financial_ratio=False`)  
6. `ingest_price_board`

- **`run_structure_full_ingestion_pipeline`**: giống trên nhưng **bật** `financial_ratio`.  
- **`run_financial_ratio_ingestion_pipeline`**: chỉ chạy financial ratio (phù hợp lịch weekly/monthly).

## 3) Chi tiết luồng theo loại dữ liệu

### OHLCV (giá cổ phiếu & chỉ số)

- **API**: `vnstock.Quote(source=..., symbol=...).history(start, end, interval="1D")`.
- **Nguồn**: thử theo thứ tự `resolved_data_sources()` — thường `primary_source` rồi `fallback_source` (mặc định `kbs` → `vci`).
- **Trong một mã**:
  1. `_resolve_*_fetch_range`: chọn `start`/`end` và `range_mode` (full N năm, incremental N ngày, bootstrap lần đầu, hoặc “full một lần” nếu bật marker).
  2. Gọi API có retry; nếu DataFrame rỗng hoặc **QC không đạt** (`validate_ohlcv_frame` với `min_rows` phụ thuộc full vs incremental) → thử nguồn kế tiếp.
  3. `build_price_like_schema` + log chất lượng → `save_partition_parquet`.

### Listing / company / financial ratio / price board

- **Listing**: `Listing` (ưu tiên `symbols_by_exchange` với tham số tương thích phiên bản vnstock). Kết quả là **snapshot**: ghi đè file master.
- **Company**: thử `Company.overview` rồi `profile` theo từng source; thứ tự source ưu tiên KBS trước (giảm lỗi tương thích VCI). **Append** vào master, có metadata lần chạy.
- **Financial ratio**: `Finance.ratio`; có retry và tùy chọn tắt nguồn khi lỗi transient / abort sau N lỗi liên tiếp (theo field trong `IngestionConfig`).
- **Price board**: `Trading.price_board` → một file snapshot theo partition ngày chạy.

## 4) Dữ liệu lấy và output

### A) Giá cổ phiếu (OHLCV)

- API: `vnstock.Quote.history`
- Output:
  - `data-lake/raw/Structure_Data/price/date=<run_date>/<TICKER>.parquet`

### B) Chỉ số (VNINDEX/VN30/HNX...)

- API: `vnstock.Quote.history`
- Output:
  - `data-lake/raw/Structure_Data/index/date=<run_date>/<INDEX_CODE>.parquet`

### C) Listing

- API: `vnstock.Listing.symbols_by_exchange` hoặc `all_symbols`
- Output (snapshot, **ghi đè** mỗi lần chạy):
  - `data-lake/raw/Structure_Data/listing/master/listing.parquet`

### D) Company overview

- API: `vnstock.Company.overview` / `profile`
- Output (**append**, giữ lịch sử các lần chạy):
  - `data-lake/raw/Structure_Data/company/master/company_overview.parquet`
  - Cột/metadata thêm: `snapshot_date`, `ingest_run_id`, `source`

### E) Financial ratio

- API: `vnstock.Finance.ratio` (period `quarter`/`year` theo code ingestor)
- Output:
  - `data-lake/raw/Structure_Data/financial_ratio/date=<run_date>/<TICKER>.parquet`

### F) Price board snapshot

- API: `vnstock.Trading.price_board`
- Output:
  - `data-lake/raw/Structure_Data/price_board/date=<run_date>/PRICE_BOARD_SNAPSHOT.parquet`

**Ghi chú `run_date`**: lấy từ `IngestionConfig.run_partition` nếu set, không thì `date.today().isoformat()`. Notebook manager thường set `run_partition` = timestamp mỗi lần chạy để tách partition giống một “DAG run”.

## 5) Cơ chế Initial Load vs Incremental (OHLCV)

### Initial Load (full history)

- Khi `use_incremental_window=False`, hoặc
- Chưa có file parquet trước đó cho mã đó (trong mọi `date=*/`) và `bootstrap_full_history_if_missing=True`
- Khoảng thời gian: từ `start_date` (≈ `years_back` năm) đến `end_date` (hôm nay)

### Incremental Load

- Khi `use_incremental_window=True`
- Nếu **đã có** file cho mã đó trong category → chỉ lấy cửa sổ `incremental_window_days` gần nhất
- QC dùng ngưỡng thấp hơn:
  - `min_ohlcv_rows_stock_incremental`
  - `min_ohlcv_rows_index_incremental`

### Full bootstrap once then incremental (tùy chọn)

- Nếu bật `full_bootstrap_once_then_incremental=True` (và vẫn bật incremental)
- Lần đầu: full history → tạo marker `_full_bootstrap_done.json` dưới `Structure_Data/<category>/`
- Các lần sau: chuyển sang incremental

## 6) Chạy từ Python

### Chạy toàn bộ (trừ financial_ratio)

- Hàm: `run_structure_ingestion_pipeline` (`pipeline.py`)

### Chạy đầy đủ (bao gồm financial_ratio)

- Hàm: `run_structure_full_ingestion_pipeline`

### Chạy riêng financial_ratio

- Hàm: `run_financial_ratio_ingestion_pipeline`

## 7) Notebook điều phối (`ingestion/ingest_structure_data_manager.ipynb`)

- **UTF-8**: cấu hình stdout/stderr và xử lý lỗi encoding trên Windows.
- **Reload module**: `importlib.reload` các submodule `structure_data` để sửa code Python không bị cache kernel.
- **`register_vnstock_api_key_from_env("VNSTOCK_API_KEY")`**: đọc `stock-pipeline/.env` (mẫu có thể tham chiếu `.env.example`).
- **Cấu hình**: gán `cfg.tickers`, `cfg.index_tickers`, `rate_limit_rpm`, `inter_request_delay_sec`, v.v.
- **Profile**: `RUN_PROFILE = "backfill" | "daily_incremental"` — áp `PROFILE_OVERRIDES` (ví dụ backfill tắt incremental; daily_incremental bật cửa sổ ngắn).
- **Thực thi**: chạy từng ô `ingest_*` hoặc một lệnh `run_structure_ingestion_pipeline(cfg)` (có nghỉ giữa nhóm API).

## 8) Hành vi ghi file (tóm tắt)

| Khu vực | Hành vi |
|---------|---------|
| `price/`, `index/`, `financial_ratio/`, `price_board/` theo `date=<run_date>/` | Mỗi lần chạy cùng `run_date`: file tương ứng được **ghi đè** (một mã một file trong partition đó). |
| `listing/master/listing.parquet` | **Ghi đè** snapshot. |
| `company/master/company_overview.parquet` | **Append** — không xóa lịch sử các lần trước. |
