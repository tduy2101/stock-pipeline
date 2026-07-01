# DBT Outputs & Lineage (Silver → Gold)

> Cập nhật: 2026-06-11

Tài liệu này mô tả **đầu ra của từng luồng dữ liệu** từ **Silver (PostgreSQL)** → **Gold (dbt)**, kèm **lineage** đến các endpoint API/UI.

**Lưu ý quan trọng:**
- Đầu ra **Gold** được xác định theo **SQL models trong `transform/dbt/models/**`**.
- File `warehouse/ddl/schema.sql` có một số bảng legacy (không phản ánh chính xác Gold hiện tại). Hãy ưu tiên dbt models.
- Runbook Bronze/Silver/load/dbt và **notebook vs mặc định DAG**: [README.md](../README.md), [Structure_data_flow.md](Structure_data_flow.md), [News_data_flow.md](News_data_flow.md), [BCTC_data_flow.md](BCTC_data_flow.md).
- **Airflow orchestration:** [docker/airflow/README_airflow.md](../docker/airflow/README_airflow.md).

## Airflow DAG → dbt subset

| DAG | Schedule (ICT) | Silver load | dbt `--select` |
|---|---|---|---|
| `structured_daily` | T2–T6 16:30 | `price,index_price,price_board` (`--latest-partitions 7`) | `fact_index_daily mart_price_board int_price_indicator fact_price_daily mart_stock_daily mart_market_overview` (incremental, không `--full-refresh`) |
| `structured_monthly` | Ngày 1 hàng tháng 17:00 | `listing,company,financial_ratio` | `+mart_financial_summary +mart_company_profile` |
| `news_daily` | Daily 06:00 | `news` | `+mart_stock_news_signal +fact_news_article` |
| `bctc_quarterly` | 15/02, 15/05, 15/08, 15/11 10:00 | `bctc_pdf_meta` | `+mart_bctc_documents` |
| `gold_full_refresh` | Daily 19:00 | — | full project (`dbt run --full-refresh` + `dbt test`) |

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

### `int_price_indicator` (incremental table)
- `ticker`, `trading_date`
- `daily_return`
- `ma7`, `ma20`, `ma50`
- `rsi14`
- `macd_line`, `macd_signal`, `macd_hist`
- `bb_middle`, `bb_upper`, `bb_lower`
- `volume_ma20`, `obv`
- `calculated_at`

### `fact_price_daily` (incremental table)
- tất cả cột từ `stg_price`
- + indicator: `daily_return`, `ma7`, `ma20`, `ma50`, `rsi14`, `macd_line`, `macd_signal`, `macd_hist`, `bb_middle`, `bb_upper`, `bb_lower`, `volume_ma20`, `obv`, `calculated_at`

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

### `mart_market_overview` (incremental table)
- `trading_date`
- `vnindex_close`, `vnindex_return`
- `vn30_close`, `vn30_return`
- `hnx_close`, `hnx_return`
- `total_market_value`, `total_volume`
- `advances`, `declines`, `unchanged`
- `top_gainers` (json), `top_losers` (json) — top 5 theo `daily_return`, tính bằng `row_number()` (không correlated subquery)
- Index: `(trading_date)` unique

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

### `mart_stock_daily` (incremental table)
- toàn bộ cột từ `fact_price_daily`
- + news fields từ `mart_stock_news_signal`: `news_count`, `avg_sentiment_score`, `weighted_sentiment`, `news_signal`, `dominant_sentiment`, `top_articles`

### `mart_price_board` (table)
- Grain: `symbol + trading_date`
- Bid/ask 3 bước, giá sàn/trần/tham chiếu, khối lượng, foreign buy/sell/room
- API: `GET /board/{symbol}`, `GET /board/{symbol}/foreign-flow`

### `mart_financial_summary` (table)
- Grain: `ticker + period_type + period`
- Pivot wide từ `stg_financial_ratio` (PE, PB, EPS, ROE, ROA, margin, growth, …)
- `period_type=annual`: suy từ quarter mới nhất trong năm nếu thiếu annual raw
- API: `GET /financials/{symbol}`

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
- `ticker` sau khi explode từ `ticker` và `ticker_mentions`
- `ticker_mentions`, `title`, `summary`, `body_text`, `url`
- `published_at`, `published_date`
- `sentiment_score`, `sentiment_label`, `word_count`, `language`
- `ticker_relevance`, `source_tier`

### `fact_news_article` (table)
- Grain hiện tại: `article_id + ticker`
- `article_id`, `ticker`, `ticker_mentions`
- `title`, `summary`, `body_text`, `url`, `source`
- `published_at`, `published_date`
- `sentiment_score`, `sentiment_label`, `word_count`, `language`
- `ticker_relevance`, `source_tier`

### `int_news_sentiment_daily` (table)
- `ticker`, `published_date`
- `news_count`, `avg_sentiment_score`
- `positive_count`, `negative_count`, `neutral_count`
- `dominant_sentiment`

### `mart_stock_news_daily` (table)

> WARNING: Deprecated: giữ lại để backward-compat. API/UI chính đã chuyển sang
> `mart_stock_news_signal`. Sẽ bỏ sau khi UI ổn định.

- `ticker`, `published_date`
- `news_count`, `avg_sentiment_score` (round)
- `positive_count`, `negative_count`, `neutral_count`
- `dominant_sentiment`

### `mart_stock_news_signal` (table)
- Nguồn: `fact_news_article` (không qua `int_news_sentiment_daily`)
- Grain: `ticker + trading_date`
- Map bài viết sang phiên giao dịch (cuối tuần / sau cutoff 14:30 ICT → phiên kế tiếp)
- `news_count`, `avg_sentiment_score`, `weighted_sentiment`
- `news_signal`: `buy_signal`, `sell_signal`, `neutral`
- `dominant_sentiment`, `top_articles` (JSONB)

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
- `doc_id`, `ticker`, `year`, `period_key`, `title`, `normalized_title`, `published_at`
- `url_pdf`, `pdf_path`, `file_size`
- `doc_class`, `canonical_priority`, `is_consolidated`
- `display_status`, `is_available_for_web`
- Filter: `display_status != 'error'` & `is_available_for_web = true`

### `mart_bctc_documents` (table)
- `doc_id`, `ticker`, `year`, `period_key`, `title`, `normalized_title`, `published_at`
- `doc_class`, `canonical_priority`, `is_consolidated`
- `display_status`, `is_available_for_web`
- `url_pdf`, `pdf_path`, `file_size`
- API list không trả `pdf_path`; endpoint file dùng `pdf_path` để stream local

---

# 4) Lineage -> Endpoint Mapping

Xem Section 7 (Current Gold/API/UI Lineage) để biết mapping chính xác nhất.

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

---

# 7) Update 2026-06-03: Current Gold/API/UI Lineage

**Silver snapshot tham chiếu (parquet, workspace):** `news/date=2026-06-03` → 924 rows;
`bctc_pdf_meta/date=2026-06-03` → 1,867 rows; `price_board/trading_date=2026-06-03` → 703 rows;
`company/current` → 703 rows. Gold row counts trong PostgreSQL cập nhật sau `load-silver` + `dbt run`.

If older sections above mention direct `stg_financial_ratio` API reads,
close-only stock charts, no price-board API, or news joined only by
`published_date`, treat those as historical notes. The current running flow is:

## 7.1 Structured marts

| Model | Grain | Current use |
|---|---|---|
| `int_price_indicator` | `ticker + trading_date` | Incremental daily (lookback 90d); full rebuild nightly. Return, MA, RSI, MACD, Bollinger, `volume_ma20`, `obv`. |
| `fact_price_daily` | `ticker + trading_date` | Incremental daily; base for `mart_stock_daily`. |
| `mart_stock_daily` | `ticker + trading_date` | Incremental daily; API `/prices/{symbol}` and `/indicators/{symbol}`; joins `mart_stock_news_signal`. |
| `mart_market_overview` | `trading_date` | Incremental daily (1 row/new session); API `/market/overview`. Full rebuild nightly. |
| `mart_price_board` | `symbol + trading_date` | API `/board/{symbol}` and `/board/{symbol}/foreign-flow`; filters by `trading_date`. |
| `mart_financial_summary` | `ticker + period_type + period` | API `/financials/{symbol}`; wide financial ratios. Quarterly rows come from source data; annual rows are derived from the latest available quarter in each year when raw annual rows are absent. |

## 7.2 News marts

| Model | Grain | Current use |
|---|---|---|
| `stg_news` | `article_id + ticker` | Explodes `ticker` plus `ticker_mentions`; adds `ticker_relevance`, `source_tier`. |
| `fact_news_article` | `article_id + ticker` | Article-level API for `/news/articles` and `/news/{symbol}/articles`; includes title, summary, body text, URL, sentiment, relevance, and source tier. |
| `int_news_sentiment_daily` | `ticker + published_date` | Legacy/simple aggregate kept for compatibility. |
| `mart_stock_news_daily` | `ticker + published_date` | Deprecated legacy/simple daily aggregate kept for backward compatibility; API/UI main flow uses `mart_stock_news_signal`. |
| `mart_stock_news_signal` | `ticker + trading_date` | Main stock-detail news source. Maps articles to trading sessions, computes `avg_sentiment_score`, `weighted_sentiment`, `news_signal`, `dominant_sentiment`, and `top_articles`. |

`mart_stock_news_signal.trading_date` can differ from article `published_date`
because weekend articles and articles after the cutoff are mapped to the next
appropriate trading session.

## 7.3 BCTC marts

| Model | Grain | Current use |
|---|---|---|
| `stg_bctc_pdf_meta` | `doc_id` | Adds `normalized_title` and `canonical_priority`. |
| `mart_bctc_documents` | `doc_id` | API `/bctc/documents` and `/bctc/{symbol}`; filters by `published_at::date`. |

## 7.4 Frontend-facing lineage

| UI area | API | Gold source | Date basis |
|---|---|---|---|
| Dashboard overview | `/market/overview?date=` | `mart_market_overview` | `trading_date` |
| Stock price chart | `/prices/{symbol}?from=&to=` | `mart_stock_daily` | `trading_date` |
| Stock indicators | `/indicators/{symbol}?from=&to=` | `mart_stock_daily` | `trading_date` |
| Stock news inline (trong price chart) | `/prices/{symbol}` | `mart_stock_daily` (join `mart_stock_news_signal`) | `trading_date` |
| Stock price board | `/board/{symbol}?from=&to=` | `mart_price_board` | `trading_date` |
| Foreign flow | `/board/{symbol}/foreign-flow?from=&to=` | `mart_price_board` | `trading_date` |
| Stock financials | `/financials/{symbol}?period_type=` | `mart_financial_summary` | `period`, `year`, `quarter` |
| Stock news signal | `/news/{symbol}?from=&to=` | `mart_stock_news_signal` | mapped `trading_date` |
| News archive / preview | `/news/articles?from=&to=` | `fact_news_article` | `published_date` |
| BCTC archive/detail | `/bctc/documents`, `/bctc/{symbol}` | `mart_bctc_documents` | `published_at::date` |

UI note: stock profile pages no longer display `charter_capital` because the
current dataset does not provide reliable display data for that field. The stock
price chart renders `open`, `high`, `low`, and `close`, with `volume` in the
hover tooltip.

## 7.5 New marts -> API mapping (verified)

| Gold mart | Endpoint(s) | Notes |
|---|---|---|
| `gold.mart_stock_news_signal` | `GET /news/{symbol}`, `GET /news/{symbol}/signal` | Main stock-news signal source; grouped by `ticker + trading_date`, includes `top_articles`. |
| `gold.mart_price_board` | `GET /board/{symbol}`, `GET /board/{symbol}/foreign-flow` | Board and foreign flow come from the same mart filtered by `trading_date`. |
| `gold.mart_financial_summary` | `GET /financials/{symbol}` | Wide-format financials; supports `period_type=quarter|annual`; annual rows are derived from latest quarter each year when raw annual rows are missing. |
