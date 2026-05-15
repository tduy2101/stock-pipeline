CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS silver.price (
  ticker text NOT NULL,
  trading_date date NOT NULL,
  open numeric(18,4),
  high numeric(18,4),
  low numeric(18,4),
  close numeric(18,4),
  volume bigint,
  value numeric(22,4),
  value_is_derived boolean DEFAULT false,
  source text,
  instrument_type text DEFAULT 'stock',
  ingested_at date,
  fetched_at timestamptz,
  is_suspicious boolean DEFAULT false,
  run_partition date,
  source_file text,
  loaded_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (ticker, trading_date)
);
SELECT create_hypertable('silver.price', 'trading_date', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS silver.index_price (
  index_code text NOT NULL,
  trading_date date NOT NULL,
  open numeric(18,4),
  high numeric(18,4),
  low numeric(18,4),
  close numeric(18,4),
  volume bigint,
  value numeric(22,4),
  value_is_derived boolean DEFAULT false,
  source text,
  instrument_type text DEFAULT 'index',
  ingested_at date,
  fetched_at timestamptz,
  is_suspicious boolean DEFAULT false,
  run_partition date,
  source_file text,
  loaded_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (index_code, trading_date)
);
SELECT create_hypertable('silver.index_price', 'trading_date', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS silver.listing (
  symbol text PRIMARY KEY,
  organ_name text,
  en_organ_name text,
  exchange text,
  type text,
  crawled_at date,
  raw_payload jsonb,
  source_file text,
  loaded_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS silver.company (
  ticker text PRIMARY KEY,
  company_name text,
  industry text,
  exchange text,
  source text,
  snapshot_date date,
  fetched_at timestamptz,
  raw_payload jsonb,
  source_file text,
  loaded_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS silver.financial_ratio (
  ticker text NOT NULL,
  period_type text NOT NULL,
  fiscal_year int NOT NULL,
  fiscal_quarter smallint NOT NULL DEFAULT 0,
  metric_name text NOT NULL,
  metric_value numeric(24,8),
  unit text,
  source text,
  run_partition date,
  raw_payload jsonb,
  source_file text,
  loaded_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (ticker, period_type, fiscal_year, fiscal_quarter, metric_name)
);

CREATE TABLE IF NOT EXISTS silver.news (
  article_id text PRIMARY KEY,
  source text,
  ticker text,
  ticker_mentions text[],
  title text NOT NULL,
  summary text,
  body_text text,
  url text,
  published_at timestamptz,
  published_date date,
  fetched_at timestamptz,
  language text DEFAULT 'vi',
  word_count int DEFAULT 0,
  sentiment_score numeric(10,4),
  sentiment_label text,
  raw_ref jsonb,
  run_partition date,
  source_file text,
  loaded_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS silver_news_url_uq ON silver.news(url) WHERE url IS NOT NULL;

CREATE TABLE IF NOT EXISTS silver.bctc_pdf_meta (
  doc_id text PRIMARY KEY,
  source text,
  ticker text,
  year int,
  period_key text,
  title text,
  published_at timestamptz,
  url_pdf text,
  url_detail text,
  pdf_path text,
  file_size bigint,
  sha256 text,
  pdf_valid_header boolean,
  qc_pass boolean,
  status text,
  error text,
  doc_class text,
  language text,
  is_consolidated boolean,
  keep_for_parse boolean,
  run_partition date,
  source_file text,
  loaded_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS silver.bctc_facts (
  doc_id text NOT NULL REFERENCES silver.bctc_pdf_meta(doc_id),
  ticker text NOT NULL,
  fiscal_year int NOT NULL,
  period text NOT NULL DEFAULT 'ANNUAL',
  statement text,
  metric_code text NOT NULL,
  metric_name text NOT NULL,
  metric_value numeric(24,4),
  unit text DEFAULT 'VND',
  page_number int,
  confidence numeric(5,4),
  parser_version text NOT NULL,
  raw_context text,
  extracted_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (doc_id, fiscal_year, period, metric_code)
);

CREATE TABLE IF NOT EXISTS silver.price_indicator (
  ticker text NOT NULL,
  trading_date date NOT NULL,
  ma7 numeric(18,4),
  ma20 numeric(18,4),
  ma50 numeric(18,4),
  rsi14 numeric(10,4),
  macd_line numeric(18,4),
  macd_signal numeric(18,4),
  macd_hist numeric(18,4),
  bb_middle numeric(18,4),
  bb_upper numeric(18,4),
  bb_lower numeric(18,4),
  calculated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (ticker, trading_date)
);
SELECT create_hypertable('silver.price_indicator', 'trading_date', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS gold.mart_stock_daily (
  ticker text NOT NULL,
  trading_date date NOT NULL,
  open numeric(18,4),
  high numeric(18,4),
  low numeric(18,4),
  close numeric(18,4),
  volume bigint,
  value numeric(22,4),
  daily_return numeric(12,6),
  ma7 numeric(18,4),
  ma20 numeric(18,4),
  ma50 numeric(18,4),
  rsi14 numeric(10,4),
  macd_line numeric(18,4),
  macd_signal numeric(18,4),
  macd_hist numeric(18,4),
  bb_middle numeric(18,4),
  bb_upper numeric(18,4),
  bb_lower numeric(18,4),
  sentiment_score numeric(10,4),
  sentiment_label text,
  news_count int DEFAULT 0,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (ticker, trading_date)
);
SELECT create_hypertable('gold.mart_stock_daily', 'trading_date', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS gold.mart_company_profile (
  ticker text PRIMARY KEY,
  organ_name text,
  en_organ_name text,
  exchange text,
  industry text,
  latest_close numeric(18,4),
  high_52w numeric(18,4),
  low_52w numeric(18,4),
  avg_volume_20d numeric(22,4),
  latest_fiscal_year int,
  revenue numeric(24,4),
  net_profit numeric(24,4),
  total_assets numeric(24,4),
  equity numeric(24,4),
  sentiment_30d numeric(10,4),
  updated_at timestamptz NOT NULL DEFAULT now()
);
