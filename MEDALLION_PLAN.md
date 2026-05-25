# Kế hoạch Medallion - Stock Pipeline

Cập nhật: 2026-05-23

File này là roadmap đầy đủ cho đồ án:
`Xây dựng hệ thống Data Pipeline & Ứng dụng Tra cứu Phân tích Thị trường Chứng khoán Việt Nam Đa nguồn`.

Mục tiêu cuối cùng: một hệ thống có thể ingest dữ liệu đa nguồn, chuẩn hóa theo Medallion Architecture, build Gold marts bằng dbt, và phục vụ ứng dụng tra cứu/phân tích cổ phiếu Việt Nam.

---

## 0. Trạng Thái Hiện Tại

### Tổng quan

- Bronze đã có 3 luồng: structured data, news, BCTC PDF.
- Silver đã tạm thời chạy được cho cả 3 luồng; structured data là luồng ổn định nhất.
- Structured Silver có 6 dataset: `price`, `index_price`, `listing`, `company`, `financial_ratio`, `price_board`.
- News Silver đã có transformer chuẩn hóa bài viết, ticker enrichment và sentiment baseline.
- BCTC Silver dừng ở mức metadata PDF để tìm kiếm/hiển thị; không parse facts tài chính trong MVP.
- Warehouse, dbt Gold, Airflow, Backend, Frontend vẫn là các module cần hoàn thiện tiếp.

### Hướng ưu tiên

Dùng structured Silver làm xuyên suốt trước, sau đó đưa news và BCTC vào Gold theo mức MVP.

Pipeline từ trái sang phải:

```
Bronze (ingest) → Silver (normalize, Python) → Warehouse (PostgreSQL loader)
→ Gold (dbt: intermediate + marts) → FastAPI → Frontend
```

`price_indicator` và `news_sentiment_daily` được tính hoàn toàn trong **dbt intermediate**, không phải Python riêng.

---

## 1. Sản Phẩm Dữ Liệu Cuối Cùng Cần Demo

Ứng dụng cuối cùng trả lời được các câu hỏi:

- Một mã cổ phiếu đang giao dịch thế nào trong ngày/năm gần đây?
- Doanh nghiệp đó là ai, niêm yết ở sàn nào, thông tin cơ bản là gì?
- Chỉ báo kỹ thuật đang xấu hay tốt?
- Tin tức gần đây về mã đó có sentiment như thế nào?
- Chỉ số thị trường VNINDEX/VN30/HNX đang thay đổi ra sao?
- Có thể tìm và mở báo cáo tài chính PDF của doanh nghiệp theo ticker/năm/kỳ không?

### Output demo cuối cùng

| Artifact | Mục đích |
|---|---|
| `gold.mart_stock_daily` | Chart giá, volume, return, chỉ báo kỹ thuật, sentiment theo ngày |
| `gold.mart_company_profile` | Profile mới nhất: tên, sàn, thông tin doanh nghiệp, giá mới nhất, ratio tài chính |
| `gold.mart_market_overview` | Snapshot tổng quan thị trường: index, thanh khoản, top movers (JSONB) |
| `gold.mart_stock_news_daily` | Tổng hợp news/sentiment theo ticker và ngày |
| `gold.mart_bctc_documents` | Danh mục PDF BCTC để tìm kiếm theo ticker/năm/kỳ |
| FastAPI (read-only Gold) | API phục vụ frontend |
| Frontend dashboard | Tra cứu ticker, chart giá, profile, news, ratio, danh sách PDF BCTC |

---

## 2. Quy Ước Medallion

### Bronze

- Giữ dữ liệu gần nguồn nhất có thể.
- Không reshape quá mạnh; chỉ thêm metadata vận hành: `source`, `fetched_at`, `ingested_at`, `snapshot_at`.
- Ghi theo partition tự nhiên của nguồn: tháng, ngày, snapshot timestamp.
- Incremental cần có watermark rõ ràng (`_watermark.json`).
- Có `_runs` log: dataset, start/end time, input params, output rows, status.

### Silver

- Chuẩn hóa schema, kiểu dữ liệu, naming, key, lineage.
- Dedupe theo grain của dataset.
- Có flag DQ cơ bản: `is_suspicious`, `value_is_derived`.
- Ghi Parquet deterministic: re-run cùng input không tạo duplicate.
- Mỗi dataset có `_runs` log riêng: input rows, output rows, output path, latest key, status.
- Có unit test cho mỗi transformer: unique key, kiểu dữ liệu, không có null ở key column.

### Gold (dbt)

- Không phản ánh source raw; phản ánh câu hỏi nghiệp vụ.
- Backend/frontend chỉ đọc Gold, không query trực tiếp Silver.
- Có dbt tests: `not_null`, `unique`, `relationships`, `accepted_values`.
- Bảng mart ưu tiên đọc nhanh, dễ query, dễ demo.
- `price_indicator` và `news_sentiment_daily` là dbt intermediate models, không phải Python script.

---

## Module 1 — Bronze Ingestion

### Mục tiêu

Thu thập dữ liệu từ các nguồn và ghi vào `data-lake/raw/`. Đảm bảo có metadata đủ để truy vết lần chạy, nguồn, snapshot và watermark.

### Trạng thái structured

| Dataset | Bronze output | Cơ chế | Trạng thái |
|---|---|---|---|
| `price` | `raw/Structure_Data/price/year=YYYY/month=MM/TICKER.parquet` | monthly per ticker, merge + overwrite, watermark | 3,050 file, 62,340 dòng, 50 mã, 2021–2026 |
| `index` | `raw/Structure_Data/index/year=YYYY/month=MM/INDEX.parquet` | monthly per index, watermark | 305 file, 6,234 dòng |
| `listing` | `raw/Structure_Data/listing/master/listing.parquet` | snapshot master overwrite | 3,253 dòng |
| `company` | `raw/Structure_Data/company/snapshots/snapshot_date=*/company_overview.parquet` | snapshot partition | 50 dòng |
| `financial_ratio` | `raw/Structure_Data/financial_ratio/snapshot_date=*/TICKER.parquet` | snapshot per ticker | 50 file, 2,547 rows |
| `price_board` | `raw/Structure_Data/price_board/snapshot_at=*/PRICE_BOARD_SNAPSHOT.parquet` | snapshot timestamp | 50 dòng |

### Output kỳ vọng cuối cùng

- Bronze Parquet cho cả 6 structured dataset.
- Bronze news RSS/HTML tại `data-lake/raw/Unstructure_Data/news/`.
- Bronze BCTC metadata/PDF tại `data-lake/raw/Semi_Structure_Data/bctc_annual_pdf*`.
- `_watermark.json` cho các dataset incremental (`price`, `index`, `financial_ratio`).
- `_runs` log cho tất cả dataset.

### Việc cần làm

- [ ] Bổ sung `_runs` log cho Bronze structured (`price`, `index`, `financial_ratio`, `price_board`).
- [ ] Ghi rõ trong README: dataset nào là daily, snapshot, weekly/monthly/manual.
- [ ] Xác nhận universe 50 ticker là demo universe cố định (không mở rộng trong MVP).

---

## Module 2 — Silver Structured Layer

### Mục tiêu

Biến raw Bronze thành Parquet analytics-ready. Đây là luồng cần đóng băng contract trước khi làm warehouse/dbt.

> Lưu ý: Bronze dataset `index` được chuẩn hóa thành Silver `index_price`.

### Trạng thái hiện tại

| Dataset | Silver output | Grain | Hiện trạng | Việc cần làm |
|---|---|---|---|---|
| `price` | `silver/price/trading_date=YYYY-MM-DD/PART-000.parquet` | `ticker + trading_date` | 62,339 dòng ✓ | thêm `_runs`; viết tests; ghi rõ `value_is_derived=true` khi `value=close*volume` |
| `index_price` | `silver/index_price/trading_date=YYYY-MM-DD/PART-000.parquet` | `index_code + trading_date` | 6,234 dòng ✓ | thêm `_runs`; viết tests |
| `listing` | `silver/listing/current/PART-000.parquet` | `symbol` | 1,535 rows ✓ | thêm `_runs`; viết tests |
| `company` | `silver/company/current/PART-000.parquet` | `ticker` | 50 dòng ✓ | thêm `_runs`; viết tests |
| `financial_ratio` | `silver/financial_ratio/period_type=quarter/year=YYYY/PART-000.parquet` | `ticker + item_code + period` | 15,282 dòng ✓ | **sửa watermark sang full timestamp**; thêm `annual`; thêm `_runs` |
| `price_board` | `silver/price_board/trading_date=YYYY-MM-DD/PART-000.parquet` | `symbol + trading_date` | 50 dòng ✓ | **chốt grain = daily latest snapshot**; cập nhật README |

### Output contract

**`silver.price`**

```
ticker, trading_date, open, high, low, close, volume, value,
value_is_derived, source, instrument_type, fetched_at,
is_suspicious, bronze_ingested_at, run_partition, source_file
```

- Unique: `ticker + trading_date`
- `value_is_derived = true` khi `value` tính từ `close * volume`
- OHLC numeric, volume integer, không null ở key columns

**`silver.index_price`**

```
index_code, trading_date, open, high, low, close, volume, value,
value_is_derived, source, instrument_type, fetched_at,
is_suspicious, bronze_ingested_at, run_partition, source_file
```

- Unique: `index_code + trading_date`

**`silver.listing`**

```
symbol, organ_name, en_organ_name, exchange, security_type,
source, crawled_at, run_partition, source_file
```

- Unique: `symbol`
- Chỉ giữ `security_type = 'stock'` cho MVP

**`silver.company`**

```
ticker, symbol, exchange, company_name, short_name, industry, sector,
charter_capital, established_year, listed_date, website, description,
source, snapshot_date, fetched_at, run_partition, source_file
```

- Unique: `ticker`
- Kiểu ngày/số đã parse được

**`silver.financial_ratio`**

```
ticker, period, period_type, year, quarter, item_code, item_name,
value, source, snapshot_date, fetched_at, run_partition, source_file
```

- Unique: `ticker + item_code + period`
- Watermark key: `snapshot_date` (full ISO timestamp, không chỉ date)
- Incremental check: `snapshot_date > last_snapshot_date` (không phải `date(snapshot_date) > last_date`)

**`silver.price_board`**

```
symbol, trading_date, exchange, reference_price, ceiling_price, floor_price,
open_price, high_price, low_price, close_price, average_price,
volume_accumulated, total_value, price_change, percent_change,
bid_price_1..3, bid_volume_1..3, ask_price_1..3, ask_volume_1..3,
foreign_buy_volume, foreign_sell_volume, foreign_net_volume,
source, snapshot_at, run_partition, source_file, is_suspicious
```

- **Grain đã chốt**: `symbol + trading_date` — giữ latest snapshot per day
- Khi chạy lại cùng ngày: overwrite partition, không tạo duplicate

### Việc cần làm

- [ ] Thêm `_runs` log cho cả 6 dataset.
- [ ] **Sửa watermark `financial_ratio`**: thay `last_date` bằng `last_snapshot_date` (full timestamp).
- [ ] **Cập nhật `price_board` transformer**: nếu có nhiều snapshot cùng ngày, giữ bản `snapshot_at` lớn nhất.
- [ ] Đồng bộ `pipeline/silver/README.md` với thực tế (hiện còn ghi `financial_ratio` và `price_board` chưa implemented).
- [ ] Viết unit tests cho `price`, `index_price`, `listing`, `company`: unique key, no null key, dtype đúng.

---

## Module 3 — Silver News Layer

### Mục tiêu

Chuẩn hóa tin tức RSS/HTML thành article table có thể join với ticker. Tạo feature sentiment baseline cho Gold.

### Output kỳ vọng cuối cùng

Path: `data-lake/silver/news/date=YYYY-MM-DD/PART-000.parquet`

```
article_id, source, ticker, ticker_mentions, title, summary, body_text,
url, published_at, published_date, fetched_at, language, word_count,
sentiment_score, sentiment_label, raw_ref, run_partition, source_file
```

- Grain: 1 dòng = 1 article duy nhất
- Unique: `article_id`; unique URL khi URL không null

### Acceptance

- Dedupe theo `article_id` (hash URL), ưu tiên row có `body_text` dài hơn, sau đó `published_at`, ưu tiên HTML.
- `ticker_mentions` được enrich từ `silver.listing` (symbol, organ_name, en_organ_name).
- Có sentiment baseline: `positive`, `neutral`, `negative`.
- Có job reprocess all Bronze news partitions (không chạy tay từng ngày).

### Việc cần làm

- [ ] Chạy lại Silver news cho tất cả partition Bronze hiện có.
- [ ] Nâng cấp ticker matching: dùng `silver.listing`, tên công ty, alias viết tắt.
- [ ] Thêm `_runs` log.

---

## Module 4 — Silver BCTC/PDF Search Layer

### Mục tiêu

Chuẩn hóa metadata BCTC PDF để tìm kiếm, lọc và hiển thị trong ứng dụng. Không OCR, không parse facts tài chính trong MVP. BCTC là luồng document search/display, không phải numeric analytics.

### Output kỳ vọng cuối cùng

Path: `data-lake/silver/bctc_pdf_meta/date=YYYY-MM-DD/PART-000.parquet`

Optional current index: `data-lake/silver/bctc_pdf_index/current/PART-000.parquet`

**`silver.bctc_pdf_meta` contract**

```
doc_id, source, ticker, year, period_key, title, published_at,
url_pdf, url_detail, pdf_path, file_size, sha256, pdf_valid_header,
qc_pass, status, error, doc_class, language, is_consolidated,
keep_for_parse, display_status, is_available_for_web,
run_partition, source_file
```

- Grain: `doc_id` (unique)
- Search fields cho web: `ticker`, `year`, `period_key`, `title`, `doc_class`, `is_consolidated`, `published_at`, `display_status`
- Display fields cho web: `doc_id`, `title`, `url_pdf`, `pdf_path`, `file_size`, `is_available_for_web`

### Acceptance

- `is_available_for_web = true` chỉ khi `qc_pass=true` AND `status='downloaded'` AND có `pdf_path` hoặc `url_pdf` hợp lệ.
- Dedupe theo `doc_id`; nếu trùng thì giữ bản mới nhất theo `published_at`.
- Pipeline không fail cả luồng khi một PDF lỗi; dòng lỗi có `display_status` và `error` rõ ràng.
- Frontend tìm được theo ticker/năm/kỳ và mở được PDF.

### Việc cần làm

- [ ] Hoàn thiện và test `pipeline/silver/bctc_pdf_meta_transformer.py`.
- [ ] Thêm `_runs` log.

---

## Module 5 — Warehouse Loader

### Mục tiêu

Load Silver Parquet vào PostgreSQL bằng idempotent upsert. Tạo schema `silver` và `gold` làm nền cho dbt/API.

> **Lưu ý TimescaleDB**: Nếu dùng TimescaleDB extension, đảm bảo `docker-compose.yml` đã bao gồm image `timescale/timescaledb-ha` và reviewer có thể chạy được. Nếu timeline eo hẹp, plain PostgreSQL với index trên `trading_date` đủ cho 62K dòng demo.

### Các bảng cần load

**Schema `silver` (load từ Parquet):**

```
silver.price
silver.index_price
silver.listing
silver.company
silver.financial_ratio
silver.price_board
silver.news
silver.bctc_pdf_meta
silver.load_audit
```

> `silver.price_indicator` và `silver.news_sentiment_daily` là **dbt intermediate models**, không load từ Parquet.

### DDL quan trọng — các column name cần đồng bộ

File `warehouse/ddl/schema.sql` phải dùng đúng tên sau (khớp Silver contract):

| Bảng | Dùng | Không dùng |
|---|---|---|
| `silver.price` | `bronze_ingested_at` | `ingested_at` |
| `silver.listing` | `security_type` | `type` |
| `silver.price_board` | grain `symbol + trading_date` | grain `symbol + snapshot_at` |

### Acceptance

- Load 2 lần không tăng duplicate (idempotent upsert theo grain key).
- DDL khớp Silver contract hiện tại.
- `silver.load_audit` ghi: dataset, partition, input rows, inserted/updated rows, status, error, timestamp.
- Hypertable TimescaleDB (nếu dùng): `silver.price`, `silver.index_price`.

### Việc cần làm

- [ ] Cập nhật `warehouse/ddl/schema.sql`: fix column names, thêm `silver.price_board`.
- [ ] Mở rộng loader tại `warehouse/loader/` (CLI `warehouse/loader/cli.py`) để load theo dataset/partition.
- [ ] Thêm bảng `silver.load_audit`.
- [ ] Chạy thử load 6 structured dataset, kiểm tra row counts.

---

## Module 6 — dbt Gold: Intermediate + Marts

### Mục tiêu

Chuyển Silver warehouse thành bảng nghiệp vụ cho API/frontend bằng dbt. Tính toán feature engineering (indicator, sentiment) hoàn toàn trong dbt intermediate — không có Python script riêng.

### Cấu trúc dbt project

```
transform/dbt/
├── models/
│   ├── intermediate/
│   │   ├── int_price_indicator.sql      ← tính MA7/20/50, RSI14, MACD, Bollinger
│   │   ├── int_news_sentiment_daily.sql ← aggregate sentiment theo ticker+date
│   │   └── int_price_board_latest.sql   ← optional: enrich price_board
│   ├── staging/
│   │   ├── stg_price.sql
│   │   ├── stg_index_price.sql
│   │   ├── stg_listing.sql
│   │   ├── stg_company.sql
│   │   ├── stg_financial_ratio.sql
│   │   ├── stg_news.sql
│   │   └── stg_bctc_pdf_meta.sql
│   └── marts/
│       ├── dim_security.sql
│       ├── dim_company.sql
│       ├── fact_price_daily.sql
│       ├── fact_index_daily.sql
│       ├── fact_financial_ratio.sql
│       ├── fact_news_article.sql
│       ├── dim_bctc_document.sql
│       ├── mart_stock_daily.sql
│       ├── mart_company_profile.sql
│       ├── mart_market_overview.sql
│       ├── mart_stock_news_daily.sql
│       └── mart_bctc_documents.sql
├── tests/
└── dbt_project.yml
```

### Intermediate models

**`int_price_indicator`**

- Source: `silver.price`
- Grain: `ticker + trading_date`
- Columns: `ticker`, `trading_date`, `daily_return`, `ma7`, `ma20`, `ma50`, `rsi14`, `macd_line`, `macd_signal`, `macd_hist`, `bb_middle`, `bb_upper`, `bb_lower`, `volatility_20d`, `calculated_at`
- Quy tắc: chỉ tính khi đủ window tối thiểu (MA50 cần 50 dòng); không có look-ahead bias (chỉ dùng dữ liệu tại ngày đó và quá khứ)

**`int_news_sentiment_daily`**

- Source: `silver.news`
- Grain: `ticker + published_date`
- Columns: `ticker`, `published_date`, `news_count`, `avg_sentiment_score`, `positive_count`, `negative_count`, `neutral_count`, `dominant_sentiment`

### Dim/Fact models

| Model | Grain | Source chính |
|---|---|---|
| `dim_security` | `symbol` | `stg_listing` |
| `dim_company` | `ticker` | `stg_company` |
| `fact_price_daily` | `ticker + trading_date` | `stg_price` + `int_price_indicator` |
| `fact_index_daily` | `index_code + trading_date` | `stg_index_price` |
| `fact_financial_ratio` | `ticker + item_code + period` | `stg_financial_ratio` |
| `fact_news_article` | `article_id` | `stg_news` |
| `dim_bctc_document` | `doc_id` | `stg_bctc_pdf_meta` |

### Mart models

**`mart_stock_daily`**

- Grain: `ticker + trading_date`
- Columns: `ticker`, `trading_date`, `open`, `high`, `low`, `close`, `volume`, `value`, `daily_return`, `ma7`, `ma20`, `ma50`, `rsi14`, `macd_line`, `macd_signal`, `macd_hist`, `bb_upper`, `bb_middle`, `bb_lower`, `volatility_20d`, `news_count`, `avg_sentiment_score`, `dominant_sentiment`

**`mart_company_profile`**

- Grain: `ticker`
- Columns: `ticker`, `symbol`, `exchange`, `company_name`, `industry`, `sector`, `listed_date`, `website`, `latest_close`, `latest_trading_date`, `high_52w`, `low_52w`, `avg_volume_20d`, `pe_ratio`, `pb_ratio`, `eps`, `roe`, `roa`

**`mart_market_overview`**

- Grain: `trading_date`
- Columns: `trading_date`, `vnindex_close`, `vnindex_return`, `vn30_close`, `vn30_return`, `hnx_close`, `hnx_return`, `total_market_value`, `total_volume`, `advances`, `declines`, `unchanged`
- `top_gainers`: `jsonb` — array 5 phần tử `{ticker, close, percent_change}`
- `top_losers`: `jsonb` — array 5 phần tử `{ticker, close, percent_change}`

> Dùng JSONB cho top movers: đơn giản, query nhanh, tránh phức tạp hóa grain.

**`mart_stock_news_daily`**

- Grain: `ticker + published_date`
- Source: `int_news_sentiment_daily`
- Columns: `ticker`, `published_date`, `news_count`, `avg_sentiment_score`, `positive_count`, `negative_count`, `neutral_count`, `dominant_sentiment`

**`mart_bctc_documents`**

- Grain: `doc_id`
- Source: `dim_bctc_document`
- Columns: `doc_id`, `ticker`, `year`, `period_key`, `title`, `published_at`, `doc_class`, `is_consolidated`, `display_status`, `is_available_for_web`, `url_pdf`, `pdf_path`, `file_size`
- Filter: chỉ giữ `display_status` không phải `'error'`

### dbt tests tối thiểu

```yaml
# price_daily
- not_null: [ticker, trading_date]
- unique: [ticker, trading_date]

# company_profile
- not_null: [ticker]
- unique: [ticker]

# market_overview
- not_null: [trading_date]
- unique: [trading_date]

# mart_stock_daily
- not_null: [ticker, trading_date]
- unique: [ticker, trading_date]
- relationships: ticker → dim_security.symbol
```

### Acceptance

- `dbt run` pass, `dbt test` pass cho key columns.
- `mart_stock_daily` có đủ 50 ticker và ngày mới nhất.
- `mart_company_profile` có đủ profile 50 ticker.
- `mart_market_overview` có top_gainers/top_losers dạng JSONB parse được.

---

## Module 7 — Airflow Orchestration

### Mục tiêu

Biến các bước ingest → silver → load → dbt thành DAG có thể demo manually.

> **Ưu tiên**: Hoàn thành FastAPI + Frontend trước. Airflow là nice-to-have cho demo. Ba DAG chạy được manual là đủ bảo vệ.

### Output kỳ vọng cuối cùng

```
dags/
├── dag_structure_daily.py
├── dag_news_daily.py
└── dag_bctc_quarterly.py
```

Airflow UI tại `http://localhost:8080`. Log từng task rõ input/output/status.

### Structure DAG (`dag_structure_daily.py`)

```
ingest_price → ingest_index → ingest_listing → ingest_company
	↓
silver_price → silver_index_price → silver_listing → silver_company
	↓
ingest_financial_ratio → silver_financial_ratio   (weekly/manual)
	↓
ingest_price_board → silver_price_board
	↓
warehouse_load_structured
	↓
dbt_run_structured   (staging + intermediate + fact + mart_stock_daily + mart_company_profile + mart_market_overview)
```

### News DAG (`dag_news_daily.py`)

```
ingest_news_rss → silver_news → warehouse_load_news → dbt_run_news
```

`dbt_run_news`: chạy `int_news_sentiment_daily` + `fact_news_article` + `mart_stock_news_daily` + update `mart_stock_daily` (join sentiment).

### BCTC DAG (`dag_bctc_quarterly.py`)

```
crawl_bctc_pdf → silver_bctc_pdf_meta → warehouse_load_bctc → dbt_run_bctc
```

`dbt_run_bctc`: chạy `dim_bctc_document` + `mart_bctc_documents`.

### Acceptance

- 3 DAG import không lỗi trong Airflow UI.
- Chạy manual từng DAG thành công trên data demo.
- Task fail có log rõ ràng và không làm hỏng partition đã thành công.

---

## Module 8 — FastAPI Backend

### Mục tiêu

Cung cấp API read-only đọc Gold cho frontend.

### Endpoints

```
GET  /health
GET  /tickers                          → danh sách 50 ticker từ mart_company_profile
GET  /market/overview?date=YYYY-MM-DD  → mart_market_overview (date mới nhất nếu không truyền)
GET  /companies/{symbol}               → mart_company_profile
GET  /prices/{symbol}?from=&to=        → mart_stock_daily (OHLCV + return), có pagination
GET  /indicators/{symbol}?from=&to=    → mart_stock_daily (chỉ indicator columns)
GET  /financials/{symbol}              → fact_financial_ratio theo ticker
GET  /news/{symbol}?from=&to=          → mart_stock_news_daily + fact_news_article, có pagination
GET  /bctc/{symbol}                    → mart_bctc_documents (danh sách PDF theo ticker)
GET  /bctc/{symbol}/file/{doc_id}      → trả FileResponse từ pdf_path hoặc redirect đến url_pdf
```

> Endpoint `/bctc/{symbol}/file/{doc_id}`: dùng `fastapi.responses.FileResponse` nếu file local, hoặc `RedirectResponse` đến `url_pdf` nếu file có URL công khai. Đây là endpoint bắt buộc để frontend mở được PDF.

### Acceptance

- API chỉ đọc schema `gold`.
- Có pagination/filter `from`/`to` cho `prices` và `news`.
- Response schema ổn định bằng Pydantic models.
- `/tickers` trả danh sách 50 ticker demo.
- `/bctc/{symbol}/file/{doc_id}` trả được file PDF hoặc redirect hợp lệ.

---

## Module 9 — Frontend

### Mục tiêu

Ứng dụng tra cứu và phân tích cổ phiếu cho người dùng demo.

### Pages và components

**Dashboard page**

- Search ticker → navigate đến detail page
- Market overview: VNINDEX/VN30/HNX close và % change
- Top gainers / top losers (đọc từ `mart_market_overview.top_gainers` JSONB)
- Latest news headlines

**Ticker detail page**

- Company profile: tên, sàn, ngành, mô tả
- Price chart: candlestick hoặc line, volume bar (đọc từ `/prices/{symbol}`)
- Technical indicators panel: MA, RSI, MACD (đọc từ `/indicators/{symbol}`)
- News panel: danh sách bài với sentiment label (đọc từ `/news/{symbol}`)
- Financial ratios: PE, PB, EPS, ROE, ROA (đọc từ `/financials/{symbol}`)
- BCTC tab: danh sách báo cáo tài chính theo năm/kỳ, nút mở PDF (đọc từ `/bctc/{symbol}`, mở qua `/bctc/{symbol}/file/{doc_id}`)

**Error/loading/empty states**: xử lý rõ ràng cho mọi component.

### Acceptance

- Search ticker đi đến detail page.
- Chart giá render được từ `/prices/{symbol}`.
- News panel đọc từ `/news/{symbol}`.
- Financial panel đọc ratio từ `/financials/{symbol}`.
- BCTC tab hiện danh sách PDF, mở được file khi click.

---

## Module 10 — Tests, Docs, Demo

### Mục tiêu

Dự án dễ chạy lại, dễ bảo vệ, dễ debug.

### Output kỳ vọng cuối cùng

- Unit tests cho Silver transformers (chạy được mà không cần API ngoài, dùng sample data nhỏ).
- Integration test: Bronze sample → Silver → Warehouse → Gold cho 1–2 ticker.
- Data dictionary cho Bronze/Silver/Gold (tên column, kiểu, mô tả, lineage field).
- Runbook `docs/RUNBOOK.md`: cách chạy từng module bằng CLI.
- Demo script `docs/DEMO.md`: thứ tự lệnh để tạo dữ liệu từ đầu và mở app.
- `README.md` root: kiến trúc tổng quan, sơ đồ pipeline, cách setup môi trường.

### Acceptance

- `pytest` pass cho các transformer quan trọng: `price`, `index_price`, `financial_ratio`, `news`.
- Sample data nhỏ (5 ticker, 30 ngày) đủ để chạy test không phụ thuộc API ngoài.
- README nói rõ kiến trúc Bronze/Silver/Gold và output từng tầng.
- Demo chạy được từ đầu với 1 lệnh hoặc một chuỗi lệnh rõ ràng.

---

## Thứ Tự Làm Tiếp

### Ưu tiên 1 — Đóng băng Silver contract (làm ngay)

1. Thêm `_runs` log cho cả 6 Silver structured dataset.
2. **Sửa watermark `financial_ratio`**: `last_date` → `last_snapshot_date` (full ISO timestamp); check `snapshot_date > last_snapshot_date`.
3. **Cập nhật `price_board` transformer**: grain `symbol + trading_date`, giữ latest snapshot per day khi overwrite.
4. Đồng bộ `pipeline/silver/README.md` với thực tế.
5. Viết unit tests cho `price`, `index_price`, `listing`, `company`.

### Ưu tiên 2 — Đưa structured Silver vào warehouse

1. **Cập nhật `warehouse/ddl/schema.sql`**: fix `bronze_ingested_at`, `security_type`, thêm `silver.price_board`.
2. Mở rộng loader tại `warehouse/loader/` với idempotent upsert.
3. Load thử 6 structured dataset, kiểm tra row counts với `silver.load_audit`.

### Ưu tiên 3 — Gold MVP cho structured (dbt)

1. Tạo staging models cho 6 structured dataset.
2. Tạo `int_price_indicator` (MA, RSI, MACD, Bollinger) trong dbt.
3. Tạo `dim_security`, `dim_company`, `fact_price_daily`, `fact_index_daily`.
4. Tạo `mart_stock_daily`, `mart_company_profile`, `mart_market_overview`.
5. Chạy `dbt test` cho key columns.

### Ưu tiên 4 — News vào Gold

1. Reprocess tất cả Silver news partition.
2. Cải thiện ticker matching dùng `silver.listing`.
3. Tạo `int_news_sentiment_daily` trong dbt.
4. Tạo `mart_stock_news_daily`.
5. Update `mart_stock_daily` join sentiment.

### Ưu tiên 5 — BCTC PDF search/display

1. Hoàn thiện `silver_bctc_pdf_meta_transformer.py`.
2. Load `silver.bctc_pdf_meta` vào warehouse.
3. Tạo `dim_bctc_document` và `mart_bctc_documents` trong dbt.

### Ưu tiên 6 — FastAPI + Frontend

1. FastAPI đọc Gold, implement tất cả endpoints kể cả `/bctc/{symbol}/file/{doc_id}`.
2. Frontend: Dashboard + Ticker detail page.
3. Kết nối đầy đủ: search, chart, indicators, news, financials, BCTC.

### Ưu tiên 7 — Airflow + Tests + Docs (hoàn thiện)

1. 3 Airflow DAG chạy manual được.
2. Integration test Bronze → Gold cho 2 ticker.
3. Runbook, Demo script, Data dictionary.

---

## Phạm Vi Future Scope (không làm trong MVP)

- Spark / Databricks / cloud deployment (MinIO, S3).
- Streaming realtime intraday.
- OCR hàng loạt PDF scan.
- Parse facts tài chính từ nội dung PDF BCTC.
- ML sentiment model nâng cao (thay rule-based baseline).
- RAG / chatbot hỏi đáp về cổ phiếu.
- Authentication cho dashboard.
- Mở rộng universe toàn bộ thị trường (hiện tại: 50 ticker demo cố định).
- SCD Type 2 cho `listing` và `company` (hiện tại: chỉ current snapshot).
