# Structure Data Ingestion

Tài liệu ngắn gọn mô tả luồng ingest dữ liệu có cấu trúc trong dự án.

## 1) Tổng quan thư mục

- `config.py`: cấu hình ingest (tickers, nguồn dữ liệu, rate limit, backfill/incremental, QC…)
- `common.py`: helper (retry, QC, schema, save parquet, env key…)
- `price_ingestor.py`: OHLCV cổ phiếu
- `index_ingestor.py`: OHLCV chỉ số
- `stock_info_ingestor.py`: listing, company overview, financial ratio, price board
- `pipeline.py`: chạy tuần tự các bước ingest

## 2) Dữ liệu lấy và output

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
- Output (snapshot, overwrite mỗi lần chạy):
  - `data-lake/raw/Structure_Data/listing/master/listing.parquet`

### D) Company overview
- API: `vnstock.Company.overview` / `profile`
- Output (append, giữ lịch sử):
  - `data-lake/raw/Structure_Data/company/master/company_overview.parquet`
  - Có thêm: `snapshot_date`, `ingest_run_id`, `source`

### E) Financial ratio
- API: `vnstock.Finance.ratio` (period `quarter`/`year`)
- Output:
  - `data-lake/raw/Structure_Data/financial_ratio/date=<run_date>/<TICKER>.parquet`

### F) Price board snapshot
- API: `vnstock.Trading.price_board`
- Output:
  - `data-lake/raw/Structure_Data/price_board/date=<run_date>/PRICE_BOARD_SNAPSHOT.parquet`

## 3) Cơ chế Initial Load vs Incremental

### Initial Load (full history)
- Khi `use_incremental_window=False`, hoặc
- Chưa có dữ liệu trước đó và `bootstrap_full_history_if_missing=True`
- Khoảng thời gian: `years_back` (mặc định 5 năm)

### Incremental Load
- Khi `use_incremental_window=True`
- Nếu đã có dữ liệu trước đó → chỉ lấy cửa sổ mới `incremental_window_days`
- QC dùng ngưỡng tối thiểu thấp hơn:
  - `min_ohlcv_rows_stock_incremental`
  - `min_ohlcv_rows_index_incremental`

### Full bootstrap once then incremental (tùy chọn)
- Nếu bật `full_bootstrap_once_then_incremental=True`
- Lần đầu chạy full → tạo marker `_full_bootstrap_done.json`
- Các lần sau tự chuyển sang incremental

## 4) Chạy pipeline

### Chạy toàn bộ (trừ financial_ratio)
- Hàm: `run_structure_ingestion_pipeline` (trong `pipeline.py`)

### Chạy đầy đủ (bao gồm financial_ratio)
- Hàm: `run_structure_full_ingestion_pipeline`

### Chạy riêng financial_ratio
- Hàm: `run_financial_ratio_ingestion_pipeline`

## 5) Notebook điều phối

Notebook: `ingestion/ingest_structure_data_manager.ipynb`
- Thiết lập config, tickers, index list
- Chọn profile `backfill` hoặc `daily_incremental`
- Gọi từng bước hoặc chạy pipeline tổng hợp
