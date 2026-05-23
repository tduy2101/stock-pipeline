-- ============================================================
-- EXTENSIONS & SCHEMAS
-- ============================================================
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- ============================================================
-- SILVER - STRUCTURED DATA
-- ============================================================

CREATE TABLE IF NOT EXISTS silver.price (
  ticker text NOT NULL,
  trading_date date NOT NULL,
  open numeric,
  high numeric,
  low numeric,
  close numeric,
  volume bigint,
  value numeric,
  value_is_derived boolean,
  source text,
  instrument_type text,
  fetched_at timestamptz,
  is_suspicious boolean,
  bronze_ingested_at text,
  run_partition text,
  source_file text,
  PRIMARY KEY (ticker, trading_date)
);
SELECT create_hypertable(
  'silver.price',
  'trading_date',
  chunk_time_interval => INTERVAL '3 months',
  if_not_exists => TRUE
);

CREATE TABLE IF NOT EXISTS silver.index_price (
  index_code text NOT NULL,
  trading_date date NOT NULL,
  open numeric,
  high numeric,
  low numeric,
  close numeric,
  volume bigint,
  value numeric,
  value_is_derived boolean,
  source text,
  instrument_type text,
  fetched_at timestamptz,
  is_suspicious boolean,
  bronze_ingested_at text,
  run_partition text,
  source_file text,
  PRIMARY KEY (index_code, trading_date)
);
SELECT create_hypertable(
  'silver.index_price',
  'trading_date',
  chunk_time_interval => INTERVAL '3 months',
  if_not_exists => TRUE
);

CREATE TABLE IF NOT EXISTS silver.listing (
  symbol text NOT NULL PRIMARY KEY,
  organ_name text,
  en_organ_name text,
  exchange text,
  security_type text,
  source text,
  crawled_at timestamptz,
  run_partition text,
  source_file text
);

CREATE TABLE IF NOT EXISTS silver.company (
  ticker text NOT NULL PRIMARY KEY,
  symbol text,
  exchange text,
  company_type text,
  business_model text,
  founded_date date,
  listing_date date,
  charter_capital bigint,
  number_of_employees bigint,
  par_value numeric,
  listing_price numeric,
  listed_volume bigint,
  outstanding_shares bigint,
  as_of_date date,
  ceo_name text,
  ceo_position text,
  auditor text,
  address text,
  phone text,
  fax text,
  email text,
  website text,
  branches text,
  history text,
  free_float_percentage numeric,
  free_float bigint,
  inspector_name text,
  inspector_position text,
  establishment_license text,
  business_code text,
  tax_id text,
  source text,
  company_method text,
  snapshot_date text,
  fetched_at timestamptz,
  run_partition text,
  source_file text
);

CREATE TABLE IF NOT EXISTS silver.financial_ratio (
  ticker text NOT NULL,
  period text NOT NULL,
  period_type text,
  year int,
  quarter int,
  item_code text NOT NULL,
  item_name text,
  value numeric,
  source text,
  snapshot_date text,
  fetched_at timestamptz,
  run_partition text,
  source_file text,
  PRIMARY KEY (ticker, item_code, period)
);

CREATE TABLE IF NOT EXISTS silver.price_board (
  symbol text NOT NULL,
  trading_date date NOT NULL,
  exchange text,
  ceiling_price numeric,
  floor_price numeric,
  reference_price numeric,
  open_price numeric,
  high_price numeric,
  low_price numeric,
  close_price numeric,
  average_price numeric,
  volume_accumulated bigint,
  total_value numeric,
  price_change numeric,
  percent_change numeric,
  bid_price_1 numeric,
  bid_vol_1 bigint,
  bid_price_2 numeric,
  bid_vol_2 bigint,
  bid_price_3 numeric,
  bid_vol_3 bigint,
  ask_price_1 numeric,
  ask_vol_1 bigint,
  ask_price_2 numeric,
  ask_vol_2 bigint,
  ask_price_3 numeric,
  ask_vol_3 bigint,
  foreign_buy_volume bigint,
  foreign_sell_volume bigint,
  foreign_room bigint,
  source text,
  snapshot_at timestamptz,
  is_suspicious boolean,
  run_partition text,
  source_file text,
  PRIMARY KEY (symbol, trading_date)
);

-- ============================================================
-- AUDIT
-- ============================================================

CREATE TABLE IF NOT EXISTS silver.load_audit (
  id serial PRIMARY KEY,
  dataset text NOT NULL,
  run_partition text,
  rows_read int,
  rows_inserted int,
  rows_updated int,
  status text NOT NULL,
  error_msg text,
  loaded_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_listing_exchange
  ON silver.listing(exchange);

CREATE INDEX IF NOT EXISTS idx_company_exchange
  ON silver.company(exchange);

CREATE INDEX IF NOT EXISTS idx_financial_ratio_ticker_period
  ON silver.financial_ratio(ticker, period_type, year, quarter);

CREATE INDEX IF NOT EXISTS idx_price_board_symbol
  ON silver.price_board(symbol);

-- ============================================================
-- SILVER - UNSTRUCTURED / SEMI-STRUCTURED DATA
-- ============================================================

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
  sentiment_score integer,
  sentiment_label text,
  sentiment_method text,
  raw_ref jsonb,
  run_partition date,
  source_file text,
  silver_loaded_at timestamptz,
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

-- ============================================================
-- GOLD
-- ============================================================

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
