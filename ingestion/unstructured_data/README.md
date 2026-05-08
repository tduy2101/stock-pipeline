# Unstructured Data (News) Ingestion

Tài liệu ngắn gọn mô tả luồng ingest tin tức trong `ingestion/unstructured_data`.

## 1) Tổng quan thư mục

- `config.py`: cấu hình ingest news (nguồn RSS/HTML, days_back, rate limit, max_per_source, ticker match…)
- `news_ingestor.py`: orchestrator (load sources → fetch → filter → dedupe → validate → save)
- `rss_adapter.py`: crawl RSS feeds
- `html_list_adapter.py`: crawl HTML list + (optional) detail page
- `schema.py`: schema chung + normalize + compute `article_id`
- `validate.py`: wrapper validate schema
- `sources.yaml`: khai báo nguồn RSS/HTML

## 2) Dữ liệu lấy & output

### Nguồn dữ liệu
- RSS feeds (từ `sources.yaml` + `rss_feed_urls`)
- HTML list pages (từ `sources.yaml`)

### Schema chuẩn (NEWS_COLUMNS)
```
article_id, source, ticker, title, summary, body_text, url,
published_at, fetched_at, language, raw_ref
```

### Output
- Mỗi nguồn ghi riêng theo ngày:
  - `data-lake/raw/Unstructure_Data/news/<source>/date=<run_date>/PART-000.parquet`
- Mặc định rerun cùng ngày sẽ **truncate** partition cũ rồi ghi mới.

## 3) Luồng xử lý

1. Load sources từ `sources.yaml`
2. Fetch RSS + HTML (nếu enable)
3. Filter theo cửa sổ ngày (`days_back_rss`, `days_back_html`)
4. Dedupe theo `article_id`
5. Validate schema
6. Save Parquet

## 4) Initial vs Incremental

- Luồng tin tức dùng **window theo ngày** chứ không backfill dài như structure data.
- **Initial**: lần đầu chạy → lấy trong `days_back` ngày gần nhất.
- **Incremental**: chạy hằng ngày → lấy lại cửa sổ `days_back` và ghi vào partition `date=<run_date>`.

## 5) Ghi chú quan trọng

- `article_id` hash từ URL (ưu tiên) hoặc metadata (source + published_at + ticker + title).
- `enable_ticker_match` cho phép map ticker từ tiêu đề/summary/body.
- `raw_ref` lưu raw payload để audit/debug.
