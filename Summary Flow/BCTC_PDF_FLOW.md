# BCTC PDF FLOW - Metadata to Gold Documents

Cap nhat den ngay 2026-05-25.

Pham vi: BCTC PDF hien chi xu ly metadata de search/filter/click/view PDF goc.
Luong nay khong OCR, khong parse bang, khong extract facts tu noi dung PDF.

## 1. Tong quan luong

```text
HNX disclosures / PDF attachments
  -> ingestion/semi_structure_data/*
  -> data-lake/raw/Semi_Structure_Data/bctc_annual_pdf*
  -> pipeline/silver/bctc_pdf_meta_transformer.py
  -> data-lake/silver/bctc_pdf_meta/*
  -> warehouse.loader.cli
  -> silver.bctc_pdf_meta
  -> dbt stg_bctc_pdf_meta
  -> gold.mart_bctc_documents
```

## 2. Code chinh

| Tang | File/module | Vai tro |
|---|---|---|
| Config | `ingestion/semi_structure_data/config.py` | Crawl/download config |
| Provider | `ingestion/semi_structure_data/providers/hnx_disclosure_provider.py` | HNX disclosure source |
| Ingest | `ingestion/semi_structure_data/bctc_annual_pdf_ingestor.py` | Crawl documents/download PDFs/write metadata |
| Pipeline | `ingestion/semi_structure_data/pipeline.py` | `run_bctc_annual_pipeline` |
| Classifier | `ingestion/semi_structure_data/document_classifier.py` | Classify doc type/canonical priority |
| Downloader | `ingestion/semi_structure_data/downloader.py` | Save PDF files |
| Silver | `pipeline/silver/bctc_pdf_meta_transformer.py` | Normalize PDF metadata |
| Loader | `warehouse/loader/silver_loader.py` | Load `silver.bctc_pdf_meta` |
| dbt | `transform/dbt/models/` | `stg_bctc_pdf_meta`, `mart_bctc_documents` |

## 3. Bronze ingest

```powershell
@'
from ingestion.semi_structure_data import SemiStructuredIngestionConfig, run_bctc_annual_pipeline

cfg = SemiStructuredIngestionConfig()
print(run_bctc_annual_pipeline(cfg, include_download=True))
'@ | python -
```

Bronze outputs:

```text
data-lake/raw/Semi_Structure_Data/bctc_annual_pdf/source=hnx/date=<YYYY-MM-DD>/ticker=<TICKER>/year=<YYYY>/<doc_id>.pdf
data-lake/raw/Semi_Structure_Data/bctc_annual_pdf_meta/source=hnx/date=<YYYY-MM-DD>/PART-000.parquet
```

Current audited Bronze partition:

```text
source=hnx/date=2026-05-14
documents crawled: 1,458
PDF files downloaded/available: 952
download failed: 0
```

## 4. Silver BCTC metadata

Run:

```powershell
python -m pipeline.silver.cli --dataset bctc_pdf_meta --run-partition 2026-05-14 --strict
```

Silver output:

```text
data-lake/silver/bctc_pdf_meta/date=2026-05-14/PART-000.parquet
data-lake/silver/bctc_pdf_meta/_runs.jsonl
```

Silver behavior:

- Reads Bronze metadata parquet only.
- Does not read PDF bytes.
- Normalizes `doc_id`, `ticker`, `year`, `period_key`, `title`, URLs, local PDF
  path, file size, status, doc class, language, and flags.
- Adds web-facing fields:
  - `display_status`
  - `is_available_for_web`
  - `keep_for_parse`
- Deduplicates by `doc_id`.

Current Silver/DB row count:

```text
silver.bctc_pdf_meta rows: 1,458
```

## 5. Load BCTC metadata vao PostgreSQL

```powershell
$env:DATABASE_URL = "postgresql://stock:stock@localhost:55432/stock_pipeline"
python -m warehouse.loader.cli load-silver --dataset bctc_pdf_meta
```

`silver.bctc_pdf_meta` grain:

```text
doc_id
```

Loader handles:

- boolean columns such as `qc_pass`, `is_consolidated`, `is_available_for_web`
- timestamps `published_at`, `silver_loaded_at`
- unique non-null `url_pdf`
- idempotent upsert by `doc_id`
- audit in `silver.load_audit`

## 6. dbt Gold BCTC documents

`stg_bctc_pdf_meta` is materialized as `ephemeral`, so it does not create a
physical table `gold.stg_bctc_pdf_meta`. It is inlined into
`mart_bctc_documents`.

Staging filter:

```sql
WHERE display_status != 'error'
  AND is_available_for_web = true
  AND doc_id IS NOT NULL
```

Current staging filter:

```text
filtered rows: 952
distinct ticker: 164
distinct doc_id: 952
```

Gold mart:

| Model | Grain | Rows hien tai | Vai tro |
|---|---|---:|---|
| `gold.mart_bctc_documents` | `doc_id` | 952 | Search/view available PDF documents |

Columns in mart:

```text
doc_id, ticker, year, period_key, title, published_at,
doc_class, is_consolidated, display_status, is_available_for_web,
url_pdf, pdf_path, file_size
```

## 7. End-to-end commands

```powershell
# 1. Bronze ingest/download
@'
from ingestion.semi_structure_data import SemiStructuredIngestionConfig, run_bctc_annual_pipeline

cfg = SemiStructuredIngestionConfig()
print(run_bctc_annual_pipeline(cfg, include_download=True))
'@ | python -

# 2. Bronze -> Silver metadata
python -m pipeline.silver.cli --dataset bctc_pdf_meta --run-partition 2026-05-14 --strict

# 3. Load DB
$env:DATABASE_URL = "postgresql://stock:stock@localhost:55432/stock_pipeline"
python -m warehouse.loader.cli load-silver --dataset bctc_pdf_meta

# 4. Build Gold
dbt run --profiles-dir transform/dbt --select mart_bctc_documents
dbt test --profiles-dir transform/dbt --select mart_bctc_documents
```

## 8. Verify nhanh

```sql
SELECT COUNT(*) FROM silver.bctc_pdf_meta;
SELECT COUNT(*) FROM gold.mart_bctc_documents;
SELECT COUNT(DISTINCT ticker) FROM gold.mart_bctc_documents;
SELECT COUNT(DISTINCT doc_id) FROM gold.mart_bctc_documents;
```

Verify mart count khop staging/source filter:

```sql
SELECT COUNT(DISTINCT doc_id)
FROM silver.bctc_pdf_meta
WHERE display_status != 'error'
  AND is_available_for_web = true
  AND doc_id IS NOT NULL;
```

Expected hien tai:

```text
gold.mart_bctc_documents rows: 952
distinct ticker: 164
distinct doc_id: 952
source-filter doc_id: 952
```

## 9. Khong lam trong flow hien tai

- Khong OCR PDF.
- Khong parse table trong PDF.
- Khong extract metric vao `silver.bctc_facts`.
- Khong download PDF tu Gold; Gold chi giu metadata/path/url cho API sau nay.

`silver.bctc_facts` la future scope neu co parser rieng sau MVP.
