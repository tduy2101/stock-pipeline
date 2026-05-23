# Audit luồng structured data cho Silver -> PostgreSQL

Ngày audit: 2026-05-20. Mọi số liệu dưới đây được đọc trực tiếp từ code và parquet trong repo tại thời điểm audit.

## Cập nhật hiện tại - Silver đã load vào PostgreSQL

Cập nhật ngày 2026-05-21: luồng structured data hiện đã đi hết từ Silver parquet sang PostgreSQL/TimescaleDB bằng loader mới.

Các phần đã implement:

- Thêm Docker TimescaleDB riêng cho project trong `docker-compose.yml`.
- PostgreSQL container: `stock-pipeline-db`.
- Host port: `55432`, không đụng PostgreSQL local đang dùng port `5432`.
- Database URL: `postgresql://stock:stock@localhost:55432/stock_pipeline`.
- Thêm setup script:
  - `warehouse/scripts/setup_db.ps1`
  - `warehouse/scripts/setup_db.sh`
- Cập nhật `warehouse/ddl/schema.sql` theo contract Silver thực tế cho 6 bảng structured:
  - `silver.price`
  - `silver.index_price`
  - `silver.listing`
  - `silver.company`
  - `silver.financial_ratio`
  - `silver.price_board`
- Thêm bảng audit load: `silver.load_audit`.
- Thêm loader mới:
  - `warehouse/loader/silver_loader.py`
  - `warehouse/loader/cli.py`
- CLI load hiện tại:

```powershell
$env:DATABASE_URL = "postgresql://stock:stock@localhost:55432/stock_pipeline"
python -m warehouse.loader.cli load-silver --dataset all
```

### Luồng dữ liệu hiện tại

Luồng structured hiện tại là:

```text
vnstock API
  -> ingestion/structure_data/*
  -> data-lake/raw/Structure_Data/*
  -> pipeline/silver/*
  -> data-lake/silver/*
  -> warehouse/loader/silver_loader.py
  -> PostgreSQL/TimescaleDB schema silver
```

Các dataset đang được load vào PostgreSQL:

| Dataset | Silver parquet | PostgreSQL table | Upsert key | Trạng thái |
|---|---|---|---|---|
| price | `data-lake/silver/price/**/*.parquet` | `silver.price` | `ticker`, `trading_date` | Đã load |
| index_price | `data-lake/silver/index_price/**/*.parquet` | `silver.index_price` | `index_code`, `trading_date` | Đã load |
| listing | `data-lake/silver/listing/**/*.parquet` | `silver.listing` | `symbol` | Đã load |
| company | `data-lake/silver/company/**/*.parquet` | `silver.company` | `ticker` | Đã load |
| financial_ratio | `data-lake/silver/financial_ratio/**/*.parquet` | `silver.financial_ratio` | `ticker`, `item_code`, `period` | Đã load |
| price_board | `data-lake/silver/price_board/**/*.parquet` | `silver.price_board` | `symbol`, `trading_date` | Đã load |

### Kết quả verify gần nhất

Kết quả verify sau khi chạy loader:

| Dataset | Parquet rows | DB rows | Latest |
|---|---:|---:|---|
| price | 62339 | 62339 | 2026-05-18 |
| index_price | 6234 | 6234 | 2026-05-18 |
| listing | 1535 | 1535 | 2026-05-18 |
| company | 50 | 50 | 2026-05-18 |
| financial_ratio | 15282 | 15282 | 2026-05-18 |
| price_board | 50 | 50 | 2026-05-18 |

Idempotency đã được kiểm tra:

- Chạy lại `price`: `rows_inserted=0`, `rows_updated=62339`.
- Chạy `load-silver --dataset all` sau khi đã load đủ: tất cả dataset đều `rows_inserted=0`, `rows_updated=N`.
- `silver.load_audit` có record `status='success'` cho đủ 6 dataset.

### Cách nhìn DB hiện tại

Dùng DBeaver/DataGrip/pgAdmin với connection:

```text
Host: localhost
Port: 55432
Database: stock_pipeline
Username: stock
Password: stock
```

Hoặc dùng terminal:

```powershell
docker exec -it stock-pipeline-db psql -U stock -d stock_pipeline
```

Một số câu query nhanh:

```sql
SELECT COUNT(*) FROM silver.price;
SELECT COUNT(*) FROM silver.index_price;
SELECT * FROM silver.load_audit ORDER BY loaded_at DESC LIMIT 10;
```

### Việc tiếp theo nên làm

Ưu tiên tiếp theo:

1. Mở DB bằng DBeaver hoặc `psql` để kiểm tra trực quan schema `silver` và dữ liệu đã load.
2. Commit lại nhóm thay đổi loader/DDL/Docker sau khi kiểm tra ổn:
   - `docker-compose.yml`
   - `warehouse/ddl/schema.sql`
   - `warehouse/loader/*`
   - `warehouse/scripts/*`
   - `tests/warehouse/*`
   - `STRUCTURED_DATA_FLOW_README.md`
3. Cập nhật tài liệu chính `README.md` hoặc `warehouse/README.md` với cách chạy DB và loader.
4. Quyết định bước Gold tiếp theo:
   - Load `silver.price` + `silver.index_price` vào mart phân tích.
   - Xây `gold.mart_stock_daily`.
   - Tính indicator/sentiment join nếu cần.
5. Tối ưu loader về incremental sau này. Hiện loader đọc toàn bộ parquet và upsert toàn bộ, chạy đúng và idempotent nhưng chưa tối ưu cho dữ liệu lớn.

## Bước 0 - Kết quả liệt kê thực tế

`bash` không có sẵn trong môi trường Windows này, nên lệnh `find` được chạy bằng PowerShell tương đương.

Code ingestion tìm thấy: `ingestion/structure_data/__init__.py`, `common.py`, `config.py`, `index_ingestor.py`, `pipeline.py`, `price_ingestor.py`, `stock_info_ingestor.py`. Không có file riêng `listing_ingestor.py`, `company_ingestor.py`, `financial_ratio_ingestor.py`, `price_board_ingestor.py`; các luồng này nằm trong `stock_info_ingestor.py`.

Code Silver tìm thấy: `pipeline/silver/__init__.py`, `bctc_pdf_meta_transformer.py`, `bronze_reader.py`, `cli.py`, `config.py`, `financial_ratio_transformer.py`, `news_transformer.py`, `news_validate.py`, `price_board_transformer.py`, `price_transformer.py`, `structure_transformer.py`, `text_utils.py`, `ticker_match.py`.

Metadata tìm thấy: `data-lake/raw/Structure_Data/_watermark.json`. Không tìm thấy `data-lake/silver/**/_runs*`. DDL hiện tại: `warehouse/ddl/schema.sql`.

Bronze partitions chính: `price/year=YYYY/month=MM` và `index/year=YYYY/month=MM` từ `2021-05` đến `2026-05`; `listing/master`; `company/snapshots/snapshot_date=2026-05-18T135703`; `financial_ratio/snapshot_date=2026-05-18T171513`; `price_board/snapshot_at=2026-05-18T07-40-08`.

Silver partitions chính: `price/trading_date=YYYY-MM-DD` và `index_price/trading_date=YYYY-MM-DD` từ `2021-05-18` đến `2026-05-18`; `listing/current`; `company/current`; `financial_ratio/period_type=quarter/year=2025|2026`; `price_board/trading_date=2026-05-18`.

Số lượng file parquet Silver: `price=1247`, `index_price=1247`, `listing=1`, `company=1`, `financial_ratio=2`, `price_board=1`.

Watermark thực tế:

```json
{
  "price": {
    "last_trading_date": "2026-05-18",
    "max_trading_date": "2026-05-18",
    "run_id": "2026-05",
    "updated_at_utc": "2026-05-18T08:55:53.526987+00:00"
  },
  "index": {
    "last_trading_date": "2026-05-18",
    "max_trading_date": "2026-05-18",
    "run_id": "2026-05",
    "updated_at_utc": "2026-05-18T08:56:02.356654+00:00"
  }
}
```

## price

### 1. Nguồn và cơ chế lấy dữ liệu

Nguồn lấy từ `vnstock.Quote(source=src, symbol=symbol).history(start, end, interval="1D")` trong `price_ingestor.py`. Thứ tự source theo config là `kbs -> vci`. Tần suất chạy không hardcode; pipeline/CLI chạy theo trigger thủ công hoặc scheduler ngoài, với `run_date` mặc định là ngày hiện tại.

Full load mặc định lấy từ `today - years_back` đến `today`, `years_back=5`. Khi `use_incremental_window=True`, code ưu tiên watermark lớn nhất từ Gold, Silver partition, raw `_watermark.json`; nếu có file tháng tồn tại thì start là ngày sau watermark. Nếu chưa có data, `bootstrap_full_history_if_missing=True` sẽ bootstrap full 5 năm.

Mỗi lần ingest ghi theo file tháng/ticker. Nếu file đã tồn tại thì merge existing + frame mới, dedupe theo `ticker + trading_date`, giữ bản ghi cuối, rồi overwrite file tháng.

### 2. Bronze

Cột raw thực tế: `time`, `open`, `high`, `low`, `close`, `volume`, `ticker`, `ingested_at`, `fetched_at`, `source`, `instrument_type`, `trading_date`.

Metadata thêm vào: `ticker`, `ingested_at=cfg.run_date`, `fetched_at=datetime.now(UTC).isoformat()`, `source`, `instrument_type=stock`; `trading_date` được suy từ cột date/time khi ghi partition.

Path: `data-lake/raw/Structure_Data/price/year=YYYY/month=MM/TICKER.parquet`. Hiện có 3050 files, 62340 dòng, range `2021-05-18 -> 2026-05-18`. Bronze có 1 duplicate key `ticker + trading_date`.

### 3. Silver

Bronze reader đọc partition tháng. Nếu không truyền `run_partition`, Silver dùng watermark từ Silver/Gold, lọc tháng >= watermark month và row có `trading_date > watermark`.

Transform chính: chuẩn hóa cột ngày thành `trading_date`, uppercase ticker, cast OHLC float, volume `Int64`, derive `value = close * volume` khi raw không có value, cast bool, flag `is_suspicious` khi giá âm hoặc `high < low`. Dedupe key `ticker + trading_date`, sort theo `fetched_at/source_file`, giữ bản ghi cuối.

Output: `data-lake/silver/price/trading_date=YYYY-MM-DD/PART-000.parquet`. Hiện có 1247 files, 62339 dòng, date range `2021-05-18 -> 2026-05-18`, 50 tickers, duplicate key = 0.

Schema thực tế: `ticker string`, `trading_date datetime64[us]`, `open/high/low/close float64`, `volume Int64`, `value Float64`, `value_is_derived boolean`, `source string`, `instrument_type string`, `fetched_at datetime64[us, UTC]`, `is_suspicious boolean`, `bronze_ingested_at object`, `run_partition object`, `source_file object`.

### 4. Sẵn sàng load PostgreSQL

Grain/upsert key: `ticker + trading_date`.

Dtype cần chú ý: `trading_date` cần insert date; `volume Int64` sang bigint; OHLC/value sang numeric; pandas nullable bool sang boolean; `fetched_at` sang timestamptz. `bronze_ingested_at` đang là token như `2026-05-18T135703`, trong DDL hiện tại lại là `ingested_at date`; `run_partition` đang là `2026-05`.

Nullable: key không null trong parquet; các cột giá/volume/value hiện không null; metadata hiện không null.

Còn thiếu/lệch chuẩn: Silver không có `_runs`; DDL `silver.price` có `ingested_at`, Silver có `bronze_ingested_at`. Cần loader mapping hoặc sửa DDL. Kết luận: sẵn sàng load về grain/data, nhưng cần map metadata trước khi insert.

## index_price

### 1. Nguồn và cơ chế lấy dữ liệu

Nguồn lấy từ `vnstock.Quote(source=src, symbol=index_code).history(start, end, interval="1D")` trong `index_ingestor.py`. Index config: `VNINDEX`, `VN30`, `HNXINDEX`, `HNX30`, `UPCOMINDEX`. Thứ tự source `kbs -> vci`; thực tế Silver có `kbs=5734`, `vci=500`.

Full/incremental giống `price`, nhưng raw dataset là `index` và Silver dataset là `index_price`. Raw watermark hiện có `index.last_trading_date=2026-05-18`.

Mỗi lần ingest merge + overwrite file tháng/index, dedupe theo `ticker + trading_date`.

### 2. Bronze

Cột raw thực tế: `time`, `open`, `high`, `low`, `close`, `volume`, `ticker`, `ingested_at`, `fetched_at`, `source`, `instrument_type`, `trading_date`.

Metadata: `ticker=index_code`, `ingested_at=cfg.run_date`, `fetched_at`, `source`, `instrument_type=index`, `trading_date` suy từ date/time.

Path: `data-lake/raw/Structure_Data/index/year=YYYY/month=MM/INDEX.parquet`. Hiện có 305 files, 6234 dòng, range `2021-05-18 -> 2026-05-18`, duplicate key = 0.

### 3. Silver

Bronze reader và transform dùng cùng logic với price, nhưng rename `ticker` thành `index_code` và yêu cầu `instrument_type=index`.

Output: `data-lake/silver/index_price/trading_date=YYYY-MM-DD/PART-000.parquet`. Hiện có 1247 files, 6234 dòng, date range `2021-05-18 -> 2026-05-18`, 5 index codes, duplicate key = 0.

Schema thực tế: `index_code string`, `trading_date datetime64[us]`, `open/high/low/close float64`, `volume Int64`, `value Float64`, `value_is_derived boolean`, `source string`, `instrument_type string`, `fetched_at datetime64[us, UTC]`, `is_suspicious boolean`, `bronze_ingested_at object`, `run_partition object`, `source_file object`.

### 4. Sẵn sàng load PostgreSQL

Grain/upsert key: `index_code + trading_date`.

Dtype và nullable tương tự `price`. Key không null; OHLC/value/volume hiện không null; `value_is_derived=True` toàn bộ vì source không trả value.

Còn thiếu/lệch chuẩn: DDL `silver.index_price` có `ingested_at`, Silver có `bronze_ingested_at`; `run_partition` đang là month string. Cần loader mapping. Kết luận: sẵn sàng load về grain/data, cần map metadata.

## listing

### 1. Nguồn và cơ chế lấy dữ liệu

Nguồn lấy từ `vnstock.Listing(source=src)`. Code thử `symbols_by_exchange(...)`, fallback `all_symbols()`. Tần suất không hardcode; là snapshot theo trigger pipeline. Không có incremental/watermark.

Mỗi lần ingest overwrite một master file: `data-lake/raw/Structure_Data/listing/master/listing.parquet`.

### 2. Bronze

Cột raw thực tế: `symbol`, `organ_name`, `en_organ_name`, `exchange`, `type`, `id`, `crawled_at`, `source`.

Metadata thêm vào: `crawled_at=cfg.run_date`, `source=src_used`. Hiện có 1 file, 3253 dòng, snapshot `2026-05-18`, duplicate symbol = 0. Null raw đang có ở `organ_name`, `en_organ_name`, `exchange`.

### 3. Silver

Bronze reader đọc full master file. Transform compact text, chuẩn hóa `symbol` uppercase, `exchange` uppercase/UNKNOWN, rename `type -> security_type`, filter chỉ lấy stock, parse `crawled_at`, dedupe theo `symbol`, giữ bản ghi cuối.

Output: `data-lake/silver/listing/current/PART-000.parquet`. Hiện có 1535 dòng, duplicate symbol = 0.

Schema thực tế: `symbol string`, `organ_name string`, `en_organ_name string`, `exchange string`, `security_type string`, `source string`, `crawled_at datetime64[us, UTC]`, `run_partition string`, `source_file string`.

### 4. Sẵn sàng load PostgreSQL

Grain/upsert key: `symbol`.

Dtype cần chú ý: `crawled_at` là timestamptz trong parquet, DDL hiện tại là date; `security_type` cần map sang cột `type` nếu giữ DDL cũ.

Nullable: Silver thực tế không null ở 9 cột trên; key không null.

Còn thiếu/lệch chuẩn: DDL `silver.listing` không có `security_type`, `source`, `run_partition`; lại có `type`, `raw_payload`. Kết luận: chưa sẵn sàng nếu load theo DDL hiện tại; cần sửa DDL hoặc mapper rõ ràng.

## company

### 1. Nguồn và cơ chế lấy dữ liệu

Nguồn lấy từ `vnstock.Company(source=src, symbol=symbol)`, thử `overview()` rồi `profile()`. Source order ưu tiên `kbs`, sau đó các source khác, `vci` cuối. Tần suất là snapshot theo trigger; không có watermark/incremental.

Mỗi lần ingest tạo snapshot mới theo `snapshot_date=cfg.run_date`, không merge với snapshot cũ.

### 2. Bronze

Cột raw thực tế: `business_model`, `symbol`, `founded_date`, `charter_capital`, `number_of_employees`, `listing_date`, `par_value`, `exchange`, `listing_price`, `listed_volume`, `ceo_name`, `ceo_position`, `inspector_name`, `inspector_position`, `establishment_license`, `business_code`, `tax_id`, `auditor`, `company_type`, `address`, `phone`, `fax`, `email`, `website`, `branches`, `history`, `free_float_percentage`, `free_float`, `outstanding_shares`, `as_of_date`, `ticker`, `source`, `company_method`, `snapshot_date`, `fetched_at`.

Metadata: `ticker`, `source`, `company_method`, `snapshot_date=cfg.run_date`, `fetched_at=datetime.now(UTC).isoformat()`.

Path: `data-lake/raw/Structure_Data/company/snapshots/snapshot_date=2026-05-18T135703/company_overview.parquet`. Hiện có 1 file, 50 dòng, duplicate ticker = 0.

### 3. Silver

Bronze reader đọc latest snapshot. Transform compact text, uppercase ticker/symbol, parse `founded_date/listing_date` day-first, parse `as_of_date/fetched_at/snapshot_date`, cast numeric, dedupe theo `ticker`, giữ bản ghi cuối theo `snapshot_date/fetched_at/source_file`.

Output: `data-lake/silver/company/current/PART-000.parquet`. Hiện có 50 dòng, duplicate ticker = 0.

Schema thực tế gồm 37 cột: `ticker`, `symbol`, `exchange`, `company_type`, `business_model`, `founded_date`, `listing_date`, `charter_capital`, `number_of_employees`, `par_value`, `listing_price`, `listed_volume`, `outstanding_shares`, `as_of_date`, `ceo_name`, `ceo_position`, `auditor`, `address`, `phone`, `fax`, `email`, `website`, `branches`, `history`, `free_float_percentage`, `free_float`, `source`, `company_method`, `snapshot_date`, `fetched_at`, `run_partition`, `source_file`, `inspector_name`, `inspector_position`, `establishment_license`, `business_code`, `tax_id`.

### 4. Sẵn sàng load PostgreSQL

Grain/upsert key: `ticker`.

Dtype cần chú ý: date/timestamp cần convert; nhiều cột int64 cần thành bigint/numeric tùy thang đo dữ liệu; text Unicode có thể giữ text.

Nullable thực tế: key không null. Cột có null: `founded_date=9`, `as_of_date=2`, `auditor=17`, `email=1`, `branches=40`, `establishment_license=16`.

Còn thiếu/lệch chuẩn: DDL `silver.company` hiện chỉ có `company_name`, `industry`, `exchange`, `source`, `snapshot_date`, `fetched_at`, `raw_payload`, `source_file`; lệch mạnh với schema Silver thực tế. Kết luận: chưa sẵn sàng load theo DDL hiện tại; cần thiết kế lại table hoặc loader chỉ load subset.

## financial_ratio

### 1. Nguồn và cơ chế lấy dữ liệu

Nguồn lấy từ `vnstock.Finance(source=src, symbol=symbol).ratio(period=period)`, thử `period=quarter` rồi `year`; hàm trả về ngay khi gặp frame đầu tiên không rỗng. Thực tế snapshot hiện tại có `ratio_period=quarter`.

Pipeline mặc định không chạy financial_ratio trong `run_structure_ingestion_pipeline(include_financial_ratio=False)`; có pipeline riêng `run_financial_ratio_ingestion_pipeline`, phù hợp weekly/monthly. Không có watermark raw; mỗi run fetch lại các ticker và ghi snapshot mới.

### 2. Bronze

Cột raw mẫu: `item`, `item_id`, `2026-Q1`, `2025-Q4`, `2025-Q4_1`, `2025-Q3`, `ticker`, `ingested_at`, `data_source`, `ratio_period`. Union thực tế còn có `2026-Q2`, `2026-Q4`, `2025-Q2`.

Metadata: `ticker`, `ingested_at=cfg.run_date`, `data_source`, `ratio_period`.

Path: `data-lake/raw/Structure_Data/financial_ratio/snapshot_date=2026-05-18T171513/TICKER.parquet`. Hiện có 50 files, 2547 raw rows.

### 3. Silver

Transformer tự đọc file theo `snapshot_date=*`, watermark bằng max `snapshot_date` đã có trong Silver. Chỉ xử lý snapshot mới hơn watermark.

Transform melt wide period columns, parse `period`, `period_type`, `year`, `quarter`; cast `value` float; map `item_id -> item_code`, `item -> item_name`, `data_source -> source`; dedupe theo `ticker + item_code + period`, giữ bản ghi mới nhất theo snapshot/source_file. Output merge vào partition cũ rồi overwrite `PART-000.parquet`.

Output: `data-lake/silver/financial_ratio/period_type=quarter/year=2025|2026/PART-000.parquet`. Hiện có 2 files, 15282 dòng, 50 tickers, 75 item_code, duplicate key = 0. Periods: `2025-Q2`, `2025-Q3`, `2025-Q4`, `2026-Q1`, `2026-Q2`, `2026-Q4`. `value` null 7641/15282 dòng.

Schema thực tế: `ticker string`, `period string`, `period_type string`, `year Int64`, `quarter Int64`, `item_code string`, `item_name string`, `value float64`, `source string`, `snapshot_date object`, `fetched_at datetime64[ns, UTC]`, `run_partition string`, `source_file string`.

### 4. Sẵn sàng load PostgreSQL

Grain/upsert key nên là `ticker + period + item_code` hoặc dạng normalized `ticker + period_type + year + quarter + item_code`.

Dtype cần chú ý: `snapshot_date` đang object date; `quarter/year` pandas nullable Int64; `value` float64 sang numeric.

Nullable: key không null; `value` null 7641 dòng. Cần quyết định load metric null hay drop null trước DB.

Còn thiếu/lệch chuẩn: DDL hiện tại dùng `fiscal_year`, `fiscal_quarter`, `metric_name`, `metric_value`, `unit`, `raw_payload`, khác schema Silver là `year`, `quarter`, `item_code`, `item_name`, `value`. Primary key DDL dùng metric name, nhưng Silver grain tốt hơn là `item_code`. Kết luận: chưa sẵn sàng load; cần fix DDL/mapping và null-value policy.

## price_board

### 1. Nguồn và cơ chế lấy dữ liệu

Nguồn lấy từ `vnstock.Trading(source=src, symbol=tickers[0]).price_board(symbols_list=tickers)`. Tần suất là snapshot theo trigger; pipeline structured mặc định có chạy `price_board`. Không có raw watermark.

Mỗi run ghi snapshot mới theo `snapshot_at=UTC timestamp`, không merge raw.

### 2. Bronze

Cột raw thực tế: `symbol`, `time`, `exchange`, `ceiling_price`, `floor_price`, `reference_price`, `open_price`, `high_price`, `low_price`, `close_price`, `average_price`, `volume_accumulated`, `total_value`, `price_change`, `percent_change`, `bid_price_1`, `bid_vol_1`, `bid_price_2`, `bid_vol_2`, `bid_price_3`, `bid_vol_3`, `ask_price_1`, `ask_vol_1`, `ask_price_2`, `ask_vol_2`, `ask_price_3`, `ask_vol_3`, `foreign_buy_volume`, `foreign_sell_volume`, `foreign_room`, `snapshot_at`, `data_source`.

Metadata: `snapshot_at`, `data_source`. Path: `data-lake/raw/Structure_Data/price_board/snapshot_at=2026-05-18T07-40-08/PRICE_BOARD_SNAPSHOT.parquet`. Hiện có 1 file, 50 dòng.

### 3. Silver

Transformer discover `snapshot_at=*`, watermark bằng max Silver `trading_date`. Chỉ chọn snapshot có `snapshot_at.date() > watermark`. Điều này có nghĩa snapshot intraday mới hơn trong cùng ngày sẽ bị skip nếu partition ngày đã tồn tại.

Transform cast numeric, `symbol` uppercase, `trading_date = snapshot_at.date`, flag suspicious khi giá không hợp lệ, dedupe theo `symbol + trading_date`, giữ latest snapshot/source. Output merge vào partition ngày rồi overwrite.

Output: `data-lake/silver/price_board/trading_date=2026-05-18/PART-000.parquet`. Hiện có 50 dòng, duplicate key = 0.

Schema thực tế: `symbol string`, `trading_date object`, `exchange string`, các cột giá `float64`, các cột volume `Int64`, `source string`, `snapshot_at datetime64[us, UTC]`, `run_partition string`, `source_file string`, `is_suspicious bool`. `ask_price_1` null 2 dòng.

### 4. Sẵn sàng load PostgreSQL

Grain/upsert key tùy mục tiêu: nếu chỉ giữ daily last snapshot thì `symbol + trading_date`; nếu cần intraday thì phải là `symbol + snapshot_at`.

Dtype cần chú ý: `trading_date` object date sang date, `snapshot_at` sang timestamptz, nullable Int64/float sang bigint/numeric.

Nullable: key không null; `ask_price_1` có null; các cột khác hiện không null.

Còn thiếu/lệch chuẩn: `warehouse/ddl/schema.sql` chưa có table `silver.price_board`. Watermark/dedup theo ngày không phù hợp nếu muốn nạp nhiều snapshot trong cùng ngày. Kết luận: chưa sẵn sàng load đến khi có DDL và chốt grain.

## Bảng tổng hợp

| Dataset | Silver grain | Upsert key | Dòng hiện tại | Mới nhất | Sẵn sàng load? | Cần fix trước |
|---|---|---|---:|---|---|---|
| price | ticker + trading_date | ticker, trading_date | 62339 | 2026-05-18 | Có điều kiện | Map `bronze_ingested_at -> ingested_at`, xử lý `run_partition=YYYY-MM`; Silver `_runs` không có |
| index_price | index_code + trading_date | index_code, trading_date | 6234 | 2026-05-18 | Có điều kiện | Map metadata tương tự price |
| listing | symbol current snapshot | symbol | 1535 | 2026-05-18 | Chưa | DDL lệch `security_type/source/run_partition`; `crawled_at` timestamp vs date |
| company | ticker current snapshot | ticker | 50 | 2026-05-18 | Chưa | DDL quá hẹp so với 37 cột Silver; cần table design/subset mapping |
| financial_ratio | ticker + item_code + period | ticker, item_code, period | 15282 | 2026-05-18 | Chưa | DDL lệch tên cột/key; quyết định load/drop `value` null |
| price_board | symbol + trading_date daily snapshot | symbol, trading_date | 50 | 2026-05-18 | Chưa | Chưa có DDL; chốt daily vs intraday grain; watermark cùng ngày |

## Thứ tự ưu tiên load DB

1. `price`: grain sạch, duplicate key = 0, DDL gần khớp nhất, là fact cốt lõi cho Gold.
2. `index_price`: cùng pattern với price, ít dòng hơn, DDL gần khớp, hữu ích làm benchmark thị trường.
3. `listing`: dimension cần cho universe ticker; chỉ cần sửa/mapping DDL nhỏ.
4. `price_board`: dữ liệu nhỏ và sạch, nhưng cần tạo DDL và chốt có giữ intraday không.
5. `company`: dữ liệu hữu ích nhưng DDL hiện tại lệch mạnh, cần quyết định load subset hay mở rộng schema.
6. `financial_ratio`: nên để sau vì schema metric/DDL chưa khớp và có nhiều `value` null cần policy rõ ràng.

## Kết luận ngắn

Luồng structured hiện tại đã có Bronze -> Silver khá rõ: OHLCV daily dùng incremental watermark theo trading date; listing/company là current snapshot; financial_ratio và price_board tự xử lý watermark bằng Silver output. Để implement loader PostgreSQL, nên bắt đầu với `price` và `index_price`, thêm mapper dtype/tên cột, rồi sửa DDL cho các dimension/snapshot còn lại trước khi nạp rộng.
