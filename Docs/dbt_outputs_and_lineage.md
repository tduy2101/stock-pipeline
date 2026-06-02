# DBT Outputs & Lineage (Silver → Gold)

> Cập nhật: 2026-06-02

Tài liệu này mô tả **đầu ra của từng luồng dữ liệu** từ **Silver (PostgreSQL)** → **Gold (dbt)**, kèm **lineage** đến các endpoint API/UI.

**Lưu ý quan trọng:**
- Đầu ra **Gold** được xác định theo **SQL models trong `transform/dbt/models/**`**.
- File `warehouse/ddl/schema.sql` có một số bảng legacy (không phản ánh chính xác Gold hiện tại). Hãy ưu tiên dbt models.

---

# 1) Structured Flow (Giá, Chỉ số, Listing, Company, Financial Ratio, Price Board)

## 1.1 Silver Tables (PostgreSQL)

### `silver.price` (grain: `ticker + trading_date`)
- `ticker`: mã cổ phiếu
- `trading_date`: ngày giao dịch
- `open`, `high`, `low`, `close`: giá
- `volume`: khối lượng
- `value`: giá trị giao dịch
- `value_is_derived`: cờ giá trị suy ra
- `source`, `instrument_type`
- `fetched_at`
- `is_suspicious`
- `bronze_ingested_at`
- `run_partition`, `source_file`

### `silver.index_price` (grain: `index_code + trading_date`)
- `index_code`: mã chỉ số
- `trading_date`
- `open`, `high`, `low`, `close`
- `volume`, `value`, `value_is_derived`
- `source`, `instrument_type`
- `fetched_at`, `is_suspicious`
- `bronze_ingested_at`
- `run_partition`, `source_file`

### `silver.listing` (grain: `symbol`)
- `symbol`
- `organ_name`, `en_organ_name`
- `exchange`, `security_type`
- `source`, `crawled_at`
- `run_partition`, `source_file`

### `silver.company` (grain: `ticker`)
- `ticker`, `symbol`, `exchange`
- `company_type`, `business_model`
- `founded_date`, `listing_date`
- `charter_capital`, `number_of_employees`
- `par_value`, `listing_price`, `listed_volume`, `outstanding_shares`
- `as_of_date`
- `ceo_name`, `ceo_position`, `auditor`
- `address`, `phone`, `fax`, `email`, `website`
- `branches`, `history`
- `free_float_percentage`, `free_float`
- `inspector_name`, `inspector_position`
- `establishment_license`, `business_code`, `tax_id`
- `source`, `company_method`
- `snapshot_date`, `fetched_at`
- `run_partition`, `source_file`

### `silver.financial_ratio` (grain: `ticker + item_code + period`)
- `ticker`
- `period`, `period_type`, `year`, `quarter`
- `item_code`, `item_name`
- `value`
- `source`, `snapshot_date`, `fetched_at`
- `run_partition`, `source_file`

### `silver.price_board` (grain: `symbol + trading_date`)
- `symbol`, `trading_date`, `exchange`
- `ceiling_price`, `floor_price`, `reference_price`
- `open_price`, `high_price`, `low_price`, `close_price`, `average_price`
- `volume_accumulated`, `total_value`
- `price_change`, `percent_change`
- `bid_price_1`, `bid_vol_1`, `bid_price_2`, `bid_vol_2`, `bid_price_3`, `bid_vol_3`
- `ask_price_1`, `ask_vol_1`, `ask_price_2`, `ask_vol_2`, `ask_price_3`, `ask_vol_3`
- `foreign_buy_volume`, `foreign_sell_volume`, `foreign_room`
- `source`, `snapshot_at`, `is_suspicious`
- `run_partition`, `source_file`

---

## 1.2 Gold Models (dbt)

### `stg_price` (view)
- `ticker`, `trading_date`
- `open`, `high`, `low`, `close` (cast double)
- `volume`, `value`, `value_is_derived`
- `source`, `instrument_type`, `fetched_at`
- `is_suspicious`, `bronze_ingested_at`
- `run_partition`, `source_file`

### `int_price_indicator` (table)
- `ticker`, `trading_date`
- `daily_return`
- `ma7`, `ma20`, `ma50`
- `rsi14`
- `macd_line`, `macd_signal`, `macd_hist`
- `bb_middle`, `bb_upper`, `bb_lower`
- `calculated_at`

### `fact_price_daily` (table)
- tất cả cột từ `stg_price`
- + indicator: `daily_return`, `ma7`, `ma20`, `ma50`, `rsi14`, `macd_line`, `macd_signal`, `macd_hist`, `bb_middle`, `bb_upper`, `bb_lower`, `calculated_at`

### `stg_index_price` (view)
- `index_code`, `trading_date`
- `open`, `high`, `low`, `close`
- `volume`, `value`, `value_is_derived`
- `source`, `instrument_type`, `fetched_at`
- `is_suspicious`, `bronze_ingested_at`
- `run_partition`, `source_file`

### `fact_index_daily` (table)
- `index_code`, `trading_date`, `open`, `high`, `low`, `close`, `volume`

### `stg_listing` (view)
- `symbol`, `organ_name`, `en_organ_name`, `exchange`, `security_type`
- `source`, `crawled_at`, `run_partition`, `source_file`

### `dim_security` (table)
- `symbol`, `organ_name`, `en_organ_name`, `exchange`, `security_type`

### `stg_company` (view)
- `ticker`, `symbol`, `exchange`
- `company_name` (null), `short_name`
- `industry`, `sector` (từ `company_type`)
- `charter_capital`, `established_year`, `listed_date`
- `website`, `description`
- `company_type`, `business_model`, `founded_date`, `listing_date`
- `number_of_employees`, `par_value`, `listing_price`, `listed_volume`, `outstanding_shares`
- `as_of_date`, `ceo_name`, `ceo_position`, `auditor`
- `address`, `phone`, `fax`, `email`, `branches`, `history`
- `free_float_percentage`, `free_float`
- `source`, `company_method`, `snapshot_date`, `fetched_at`
- `run_partition`, `source_file`

### `dim_company` (table)
- `ticker`, `symbol`, `exchange`
- `company_name` (coalesce từ listing)
- `short_name`, `industry`, `sector`
- `charter_capital`, `established_year`, `listed_date`
- `website`, `description`
- `organ_name`, `en_organ_name`

### `stg_financial_ratio` (view)
- `ticker`, `period`, `period_type`, `year`, `quarter`
- `item_code`, `item_name`, `value`
- `source`, `snapshot_date`, `fetched_at`, `run_partition`, `source_file`

### `mart_company_profile` (table)
- `ticker`, `symbol`, `exchange`
- `company_name`, `short_name`, `industry`, `sector`
- `charter_capital`, `established_year`, `listed_date`
- `website`, `description`
- `organ_name`, `en_organ_name`
- `latest_close`, `latest_trading_date`
- `high_52w`, `low_52w`
- `avg_volume_20d`
- `pe_ratio`, `pb_ratio`, `eps`, `roe`, `roa`

### `mart_market_overview` (table)
- `trading_date`
- `vnindex_close`, `vnindex_return`
- `vn30_close`, `vn30_return`
- `hnx_close`, `hnx_return`
- `total_market_value`, `total_volume`
- `advances`, `declines`, `unchanged`
- `top_gainers` (json), `top_losers` (json)

### `stg_price_board` (view)
- `symbol`, `trading_date`, `exchange`
- `ceiling_price`, `floor_price`, `reference_price`
- `open_price`, `high_price`, `low_price`, `close_price`, `average_price`
- `volume_accumulated`, `total_value`
- `price_change`, `percent_change`
- `bid_price_1`, `bid_vol_1`, `bid_price_2`, `bid_vol_2`, `bid_price_3`, `bid_vol_3`
- `ask_price_1`, `ask_vol_1`, `ask_price_2`, `ask_vol_2`, `ask_price_3`, `ask_vol_3`
- `foreign_buy_volume`, `foreign_sell_volume`, `foreign_room`
- `source`, `snapshot_at`, `is_suspicious`
- `run_partition`, `source_file`

### `mart_stock_daily` (table)
- toàn bộ cột từ `fact_price_daily`
- + `news_count`, `avg_sentiment_score`, `dominant_sentiment`

### `mart_ticker_directory` (table)
- `ticker`, `exchange`, `organ_name`, `en_organ_name`, `security_type`
- `company_name`, `short_name`
- `has_full_profile`, `has_price`, `has_news`, `has_bctc`
- `news_count`, `bctc_doc_count`
- `latest_trading_date`, `latest_news_at`, `latest_bctc_at`

---

# 2) News Flow

## 2.1 Silver Table

### `silver.news` (grain: `article_id`)
- `article_id`, `source`
- `ticker`, `ticker_mentions`
- `title`, `summary`, `body_text`, `url`
- `published_at`, `published_date`
- `fetched_at`
- `language`, `word_count`
- `sentiment_score`, `sentiment_label`, `sentiment_method`
- `raw_ref`
- `run_partition`, `source_file`, `silver_loaded_at`, `loaded_at`

---

## 2.2 Gold Models

### `stg_news` (ephemeral)
- `article_id`, `source`
- `ticker` (coalesce từ `ticker` hoặc `ticker_mentions[1]`)
- `ticker_mentions`, `title`, `summary`, `body_text`, `url`
- `published_at`, `published_date`
- `sentiment_score`, `sentiment_label`, `word_count`, `language`

### `fact_news_article` (table)
- `article_id`, `ticker`, `ticker_mentions`
- `title`, `summary`, `body_text`, `url`, `source`
- `published_at`, `published_date`
- `sentiment_score`, `sentiment_label`, `word_count`, `language`

### `int_news_sentiment_daily` (table)
- `ticker`, `published_date`
- `news_count`, `avg_sentiment_score`
- `positive_count`, `negative_count`, `neutral_count`
- `dominant_sentiment`

### `mart_stock_news_daily` (table)
- `ticker`, `published_date`
- `news_count`, `avg_sentiment_score` (round)
- `positive_count`, `negative_count`, `neutral_count`
- `dominant_sentiment`

---

# 3) BCTC PDF Flow

## 3.1 Silver Table

### `silver.bctc_pdf_meta` (grain: `doc_id`)
- `doc_id`, `source`, `ticker`, `year`, `period_key`
- `title`, `normalized_title`
- `published_at`
- `url_pdf`, `url_detail`
- `pdf_path`, `file_size`, `sha256`
- `pdf_valid_header`, `qc_pass`
- `status`, `error`
- `doc_class`, `language`
- `is_consolidated`, `is_explanation`, `is_disclosure`
- `canonical_priority`, `keep_for_parse`
- `display_status`, `is_available_for_web`
- `run_partition`, `source_file`, `silver_loaded_at`, `loaded_at`

---

## 3.2 Gold Models

### `stg_bctc_pdf_meta` (ephemeral)
- `doc_id`, `ticker`, `year`, `period_key`, `title`, `published_at`
- `url_pdf`, `pdf_path`, `file_size`
- `doc_class`, `is_consolidated`
- `display_status`, `is_available_for_web`
- Filter: `display_status != 'error'` & `is_available_for_web = true`

### `mart_bctc_documents` (table)
- `doc_id`, `ticker`, `year`, `period_key`, `title`, `published_at`
- `doc_class`, `is_consolidated`
- `display_status`, `is_available_for_web`
- `url_pdf`, `pdf_path`, `file_size`

---

# 4) Lineage → Endpoint Mapping

## 4.1 Giá & chỉ báo
- `GET /prices/{symbol}` → `gold.mart_stock_daily`
- `GET /indicators/{symbol}` → `gold.mart_stock_daily`

## 4.2 Hồ sơ công ty
- `GET /companies/{symbol}` → `gold.mart_company_profile`
- `GET /financials/{symbol}` → `gold.stg_financial_ratio`

## 4.3 Thị trường
- `GET /market/overview` → `gold.mart_market_overview`

## 4.4 News
- `GET /news/articles` → `gold.fact_news_article`
- `GET /news/{symbol}/articles` → `gold.fact_news_article`
- `GET /news/{symbol}` → `gold.mart_stock_news_daily`
- `GET /news/market` → `gold.fact_news_article`

## 4.5 BCTC
- `GET /bctc/documents` → `gold.mart_bctc_documents`
- `GET /bctc/recent` → `gold.mart_bctc_documents`
- `GET /bctc/{symbol}` → `gold.mart_bctc_documents`
- `GET /bctc/{symbol}/file/{doc_id}` → lookup `gold.mart_bctc_documents.pdf_path`

---

# 5) Gợi ý layout UI (Search ticker → News + BCTC)

**Search Box**
- Nguồn: `gold.mart_ticker_directory`
- Hiển thị: `ticker`, `organ_name`, `exchange`, `has_news`, `has_bctc`, `latest_news_at`, `latest_bctc_at`

**Ticker Detail**
- Header: `gold.mart_company_profile`
- Chart: `gold.mart_stock_daily`
- News Tab: `gold.fact_news_article` (filter theo `ticker` / `ticker_mentions`)
- BCTC Tab: `gold.mart_bctc_documents`

---

# 6) Ghi chú gửi Claude

- Model & columns lấy từ `transform/dbt/models/**` (DBT SQL hiện tại).
- Nếu chỉnh output layout, hãy dựa vào các bảng Gold ở mục 1–3.
- Khi thay đổi UI search, ưu tiên `mart_ticker_directory` làm nguồn aggregator.
