# STRUCTURED DATA FLOW - Bronze to Silver to Gold

Cap nhat den ngay 2026-05-25.

Pham vi: structured data gom gia co phieu, gia chi so, listing, company overview,
financial ratio, va price board. Luong nay da di het den PostgreSQL schema
`silver` va dbt schema `gold`.

## 1. Tong quan luong

```text
vnstock API
  -> ingestion/structure_data/*
  -> data-lake/raw/Structure_Data/*
  -> pipeline/silver/*
  -> data-lake/silver/*
  -> warehouse.loader.cli
  -> PostgreSQL schema silver
  -> dbt staging/intermediate/marts
  -> PostgreSQL schema gold
```

## 2. Code chinh

| Tang | File/module | Vai tro |
|---|---|---|
| Bronze ingest | `ingestion/structure_data/pipeline.py` | Orchestrate structured ingest |
| Bronze price | `ingestion/structure_data/price_ingestor.py` | OHLCV stock price |
| Bronze index | `ingestion/structure_data/index_ingestor.py` | OHLCV index price |
| Bronze stock info | `ingestion/structure_data/stock_info_ingestor.py` | listing, company, ratio, price board |
| Silver price/index | `pipeline/silver/price_transformer.py` | Normalize OHLCV |
| Silver listing/company | `pipeline/silver/structure_transformer.py` | Current dimensions |
| Silver ratio | `pipeline/silver/financial_ratio_transformer.py` | Wide-to-long ratio metrics |
| Silver board | `pipeline/silver/price_board_transformer.py` | Daily latest board snapshot |
| Silver CLI | `pipeline/silver/cli.py` | `python -m pipeline.silver.cli` |
| Loader | `warehouse/loader/silver_loader.py` | Parquet -> PostgreSQL upsert |
| dbt | `transform/dbt/models/` | Build Gold models |

## 3. Bronze

Run full structured Bronze ingest:

```powershell
@'
from ingestion.structure_data import IngestionConfig, run_structure_full_ingestion_pipeline

cfg = IngestionConfig()
print(run_structure_full_ingestion_pipeline(cfg))
'@ | python -
```

Common outputs:

```text
data-lake/raw/Structure_Data/price/year=<YYYY>/month=<MM>/<TICKER>.parquet
data-lake/raw/Structure_Data/index/year=<YYYY>/month=<MM>/<INDEX_CODE>.parquet
data-lake/raw/Structure_Data/listing/master/listing.parquet
data-lake/raw/Structure_Data/company/snapshots/snapshot_date=<date>/company_overview.parquet
data-lake/raw/Structure_Data/financial_ratio/snapshot_date=<timestamp>/<TICKER>.parquet
data-lake/raw/Structure_Data/price_board/snapshot_at=<timestamp>/PRICE_BOARD_SNAPSHOT.parquet
```

Bronze giu du lieu gan nguon nhat co the, co metadata ingest/source. Bronze
khong load truc tiep vao DB.

## 4. Silver

Run all structured Silver transforms:

```powershell
python -m pipeline.silver.cli --dataset all --strict
```

Run one dataset:

```powershell
python -m pipeline.silver.cli --dataset price --strict
python -m pipeline.silver.cli --dataset index_price --strict
python -m pipeline.silver.cli --dataset listing --strict
python -m pipeline.silver.cli --dataset company --strict
python -m pipeline.silver.cli --dataset financial_ratio --strict
python -m pipeline.silver.cli --dataset price_board --strict
```

Silver outputs:

```text
data-lake/silver/price/trading_date=<YYYY-MM-DD>/PART-000.parquet
data-lake/silver/index_price/trading_date=<YYYY-MM-DD>/PART-000.parquet
data-lake/silver/listing/current/PART-000.parquet
data-lake/silver/company/current/PART-000.parquet
data-lake/silver/financial_ratio/period_type=<quarter|annual>/year=<YYYY>/PART-000.parquet
data-lake/silver/price_board/trading_date=<YYYY-MM-DD>/PART-000.parquet
data-lake/silver/<dataset>/_runs.jsonl
```

Important data correctness rules:

- `financial_ratio` watermark dung full `snapshot_date` token, vi du
  `2026-05-18T171513`, khong cat ve date-only.
- `price_board` grain la `symbol + trading_date`; neu co nhieu snapshot trong
  cung ngay thi giu row co `snapshot_at` lon nhat.
- Silver rerun overwrite target partition/current folder, khong append file moi.

## 5. Load Silver vao PostgreSQL

Start DB/apply DDL:

```powershell
.\warehouse\scripts\setup_db.ps1
```

Load structured datasets:

```powershell
$env:DATABASE_URL = "postgresql://stock:stock@localhost:55432/stock_pipeline"
python -m warehouse.loader.cli load-silver --dataset price,index_price,listing,company,financial_ratio,price_board
```

Hoac load tat ca dataset Silver hien duoc support:

```powershell
python -m warehouse.loader.cli load-silver --dataset all
```

Loader doc tat ca parquet theo glob trong `DATASET_CONFIG`, normalize dtype,
validate key, upsert vao table dich va ghi audit vao `silver.load_audit`.

## 6. Schema silver hien tai

| Table | Grain/upsert key | Rows hien tai |
|---|---|---:|
| `silver.price` | `ticker + trading_date` | 62,339 |
| `silver.index_price` | `index_code + trading_date` | 6,234 |
| `silver.listing` | `symbol` | 1,535 |
| `silver.company` | `ticker` | 50 |
| `silver.financial_ratio` | `ticker + item_code + period` | 15,282 |
| `silver.price_board` | `symbol + trading_date` | 50 |

Audit loader:

```sql
SELECT dataset, rows_read, rows_inserted, rows_updated, status, loaded_at
FROM silver.load_audit
WHERE dataset IN ('price','index_price','listing','company','financial_ratio','price_board')
ORDER BY loaded_at DESC
LIMIT 20;
```

## 7. dbt Gold tu structured

Run dbt tu repo root:

```powershell
dbt run --profiles-dir transform/dbt
dbt test --profiles-dir transform/dbt
```

Structured Gold models:

| Gold model | Source chinh | Grain | Rows hien tai |
|---|---|---|---:|
| `gold.stg_price` | `silver.price` | `ticker + trading_date` | 62,339 |
| `gold.stg_index_price` | `silver.index_price` | `index_code + trading_date` | 6,234 |
| `gold.stg_listing` | `silver.listing` | `symbol` | 1,535 |
| `gold.stg_company` | `silver.company` | `ticker` | 50 |
| `gold.stg_financial_ratio` | `silver.financial_ratio` | `ticker + item_code + period` | 15,282 |
| `gold.stg_price_board` | `silver.price_board` | `symbol + trading_date` | 50 |
| `gold.int_price_indicator` | `stg_price` | `ticker + trading_date` | 62,339 |
| `gold.dim_security` | `stg_listing` | `symbol` | 1,535 |
| `gold.dim_company` | `stg_company + stg_listing` | `ticker` | 50 |
| `gold.fact_price_daily` | `stg_price + int_price_indicator` | `ticker + trading_date` | 62,339 |
| `gold.fact_index_daily` | `stg_index_price` | `index_code + trading_date` | 6,234 |
| `gold.mart_stock_daily` | `fact_price_daily + news sentiment` | `ticker + trading_date` | 62,339 |
| `gold.mart_company_profile` | `dim_company + price + ratio` | `ticker` | 50 |
| `gold.mart_market_overview` | `fact_index_daily + fact_price_daily` | `trading_date` | 1,247 |

## 8. Indicator va ratios

`int_price_indicator` tinh technical indicators tren `stg_price`:

- daily return
- MA7, MA20, MA50
- RSI14 theo Cutler/SMA rolling avg
- MACD SMA approximation
- Bollinger Bands

`mart_company_profile` lay financial ratio tu `stg_financial_ratio`:

```text
pe_ratio <- item_code pe_ratio
pb_ratio <- item_code pb_ratio
eps      <- item_code trailing_eps
roe      <- item_code roe
roa      <- item_code roa
```

Model lay latest period co data non-null theo tung ticker. Verify gan nhat:

```text
total=50, has_pe=50, has_pb=50, has_eps=50, has_roe=50, has_roa=50
```

## 9. Verify nhanh

```sql
SELECT COUNT(*) FROM gold.fact_price_daily;
SELECT COUNT(*) FROM gold.mart_stock_daily;
SELECT COUNT(*) FROM gold.mart_company_profile;

SELECT ticker, latest_close, pe_ratio, pb_ratio, eps, roe, roa
FROM gold.mart_company_profile
WHERE ticker IN ('VCB', 'FPT', 'VNM', 'HPG', 'SSI')
ORDER BY ticker;
```

## 10. Ghi chu

- `silver.price_indicator` khong duoc load tu Python. Indicator nam o dbt model
  `gold.int_price_indicator`.
- `silver.bctc_facts` khong thuoc structured MVP hien tai.
- FastAPI/React chua implement; Gold marts la contract dau vao cho buoc API sau.
