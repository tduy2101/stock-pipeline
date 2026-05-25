# GOLD DBT FLOW

Cap nhat den ngay 2026-05-25 trong workspace:

```text
d:\WorkSpace\Do An 2\stock-pipeline
```

Pham vi tai lieu nay: cach dbt doc schema `silver` trong PostgreSQL va tao cac
model schema `gold` hien tai. Bronze va Silver parquet khong duoc dbt doc truc
tiep; buoc dua parquet vao DB la `warehouse.loader.cli`.

## 1. Vi tri project dbt

```text
dbt_project.yml                 # root launcher, chay duoc tu repo root
transform/dbt/dbt_project.yml    # dbt project khi cd vao transform/dbt
transform/dbt/profiles.yml       # postgres target dev
transform/dbt/models/staging/
transform/dbt/models/intermediate/
transform/dbt/models/marts/
transform/dbt/tests/generic/
```

`profiles.yml` hien tro vao DB local:

```yaml
host: localhost
port: 55432
user: stock
password: stock
dbname: stock_pipeline
schema: gold
threads: 4
```

## 2. Cach chay

Tu repo root:

```powershell
dbt debug --profiles-dir transform/dbt
dbt compile --profiles-dir transform/dbt
dbt run --profiles-dir transform/dbt
dbt test --profiles-dir transform/dbt
```

Tu trong thu muc dbt:

```powershell
cd transform/dbt
dbt run --profiles-dir .
dbt test --profiles-dir .
cd ../..
```

Chay rieng mot model:

```powershell
dbt run --profiles-dir transform/dbt --select mart_company_profile
dbt test --profiles-dir transform/dbt --select mart_company_profile
```

## 3. dbt hoat dong nhu the nao trong project nay

dbt khong upload file parquet vao DB. Luong dung la:

```text
data-lake/silver/*.parquet
  -> python -m warehouse.loader.cli load-silver --dataset all
  -> schema silver trong PostgreSQL
  -> dbt source('silver', ...)
  -> staging
  -> intermediate
  -> marts/facts/dimensions trong schema gold
```

Co 2 loai dependency:

- `source('silver', 'price')`: doc table da load san trong schema `silver`.
- `ref('stg_price')`: tham chieu model dbt khac, de dbt sap xep DAG va build
  dung thu tu.

`dbt run` tao view/table theo config materialization. `dbt test` chay cac test
trong `schema.yml` va generic test tu `tests/generic`.

## 4. Sources

File:

```text
transform/dbt/models/staging/sources.yml
```

Source schema:

```text
silver
```

Tables dang duoc dbt doc:

```text
silver.price
silver.index_price
silver.listing
silver.company
silver.financial_ratio
silver.price_board
silver.news
silver.bctc_pdf_meta
```

## 5. Staging

Structured staging materialized as view:

| Model | Source | Grain | Current rows |
|---|---|---|---:|
| `stg_price` | `silver.price` | `ticker + trading_date` | 62,339 |
| `stg_index_price` | `silver.index_price` | `index_code + trading_date` | 6,234 |
| `stg_listing` | `silver.listing` | `symbol` | 1,535 |
| `stg_company` | `silver.company` | `ticker` | 50 |
| `stg_financial_ratio` | `silver.financial_ratio` | `ticker + item_code + period` | 15,282 |
| `stg_price_board` | `silver.price_board` | `symbol + trading_date` | 50 |

News/BCTC staging override materialization to `ephemeral`, nen khong tao physical
relation trong `gold`:

| Model | Source | Filter | Filtered rows |
|---|---|---|---:|
| `stg_news` | `silver.news` | `published_date`, `ticker`, `article_id` not null | 173 |
| `stg_bctc_pdf_meta` | `silver.bctc_pdf_meta` | `display_status != 'error'`, `is_available_for_web = true`, `doc_id` not null | 952 |

## 6. Intermediate

| Model | Source | Grain | Current rows |
|---|---|---|---:|
| `int_price_indicator` | `stg_price` | `ticker + trading_date` | 62,339 |
| `int_news_sentiment_daily` | `stg_news` | `ticker + published_date` | 154 |

`int_price_indicator` tinh:

- `daily_return`
- `ma7`, `ma20`, `ma50`
- `rsi14` theo Cutler/SMA method
- `macd_line`, `macd_signal`, `macd_hist` theo SMA approximation
- `bb_middle`, `bb_upper`, `bb_lower`

`int_news_sentiment_daily` aggregate:

- `news_count`
- `avg_sentiment_score`
- `positive_count`, `negative_count`, `neutral_count`
- `dominant_sentiment`

## 7. Marts / facts / dimensions

| Model | Grain | Inputs | Current rows |
|---|---|---|---:|
| `dim_security` | `symbol` | `stg_listing` | 1,535 |
| `dim_company` | `ticker` | `stg_company` + `stg_listing` | 50 |
| `fact_price_daily` | `ticker + trading_date` | `stg_price` + `int_price_indicator` | 62,339 |
| `fact_index_daily` | `index_code + trading_date` | `stg_index_price` | 6,234 |
| `mart_stock_daily` | `ticker + trading_date` | `fact_price_daily` + `int_news_sentiment_daily` | 62,339 |
| `mart_company_profile` | `ticker` | `dim_company` + `fact_price_daily` + `stg_financial_ratio` | 50 |
| `mart_market_overview` | `trading_date` | `fact_index_daily` + `fact_price_daily` | 1,247 |
| `mart_stock_news_daily` | `ticker + published_date` | `int_news_sentiment_daily` | 154 |
| `mart_bctc_documents` | `doc_id` | `stg_bctc_pdf_meta` | 952 |

## 8. Mart chi tiet

### `fact_price_daily`

Main price fact. Giu OHLCV va metadata tu `stg_price`, join them cac cot
indicator tu `int_price_indicator`.

### `mart_stock_daily`

Main daily mart cho API/dashboard stock time series:

- Tat ca cot tu `fact_price_daily`.
- `news_count` tu `int_news_sentiment_daily`, default 0 neu khong co news.
- `avg_sentiment_score`.
- `dominant_sentiment`.

### `mart_company_profile`

Company profile mart:

- Profile fields tu `dim_company`.
- Latest close va latest trading date.
- 52-week high/low theo max trading date trong price fact.
- Average volume 20 trading days gan nhat.
- Financial ratios tu `stg_financial_ratio`:
  - `pe_ratio`
  - `pb_ratio`
  - `eps` tu item_code `trailing_eps`
  - `roe`
  - `roa`

Model lay latest period co data non-null theo tung ticker de tranh period tuong
lai/placeholder toan NULL.

### `mart_market_overview`

Market overview theo ngay:

- Pivot index close/return cho `VNINDEX`, `VN30`, `HNXINDEX`.
- Tong volume/value.
- Advances/declines/unchanged.
- `top_gainers`, `top_losers` dang JSON array.

### `mart_stock_news_daily`

Mart news sentiment daily theo ticker/date, dung cho API news analytics hoac join
vao stock daily.

### `mart_bctc_documents`

Mart search/view PDF:

- `doc_id`, `ticker`, `year`, `period_key`
- title, published timestamp, doc class
- consolidated flag
- url/pdf path/file size

Chi gom docs available for web, khong gom `display_status = 'error'`.

## 9. Tests

Staging tests:

- not null key columns
- unique keys
- `unique_combination_of_columns` cho composite grains

Marts tests:

- `fact_price_daily`: unique `ticker + trading_date`
- `dim_security`: unique `symbol`
- `dim_company`: unique `ticker`
- `mart_stock_daily`: unique `ticker + trading_date`, relationship ticker ->
  `dim_security.symbol`
- `mart_company_profile`: unique `ticker`
- `mart_market_overview`: unique `trading_date`
- `mart_stock_news_daily`: unique `ticker + published_date`
- `mart_bctc_documents`: unique `doc_id`
- `int_news_sentiment_daily`: unique `ticker + published_date`

Current result:

```text
dbt test --profiles-dir transform/dbt
PASS=48 WARN=0 ERROR=0
```

## 10. Verify nhanh

```sql
SELECT COUNT(*) FROM gold.mart_stock_daily;
SELECT COUNT(*) FROM gold.mart_company_profile;
SELECT COUNT(*) FROM gold.mart_market_overview;
SELECT COUNT(*) FROM gold.mart_stock_news_daily;
SELECT COUNT(*) FROM gold.mart_bctc_documents;
```

Verify news da join vao stock daily:

```sql
SELECT ticker, trading_date, news_count, dominant_sentiment
FROM gold.mart_stock_daily
WHERE news_count > 0
ORDER BY trading_date DESC, ticker
LIMIT 5;
```

Verify ratio da join vao company profile:

```sql
SELECT
  COUNT(*) AS total,
  COUNT(pe_ratio) AS has_pe,
  COUNT(pb_ratio) AS has_pb,
  COUNT(eps) AS has_eps,
  COUNT(roe) AS has_roe,
  COUNT(roa) AS has_roa
FROM gold.mart_company_profile;
```

Ket qua hien tai:

```text
total=50, has_pe=50, has_pb=50, has_eps=50, has_roe=50, has_roa=50
```

## 11. Khong nam trong dbt hien tai

- Khong co model OCR/PDF fact extraction.
- Khong tao `silver.price_indicator`; indicator nam trong `gold.int_price_indicator`.
- Khong tao `silver.bctc_facts`; BCTC hien la metadata/search document.
- Chua co FastAPI/Airflow/React.
