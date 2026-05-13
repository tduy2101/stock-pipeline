# Semi-structured ingestion: BCTC nam (PDF-first)

Module nay thu thap Bao cao tai chinh nam (BCTC nam) theo huong semi-structured.

## Stage 1 - Crawl + Download PDF + Metadata

- Lay danh sach cong bo tu provider (`hnx` o phase dau).
- Loc tai lieu BCTC nam theo keyword.
- Download PDF theo `requests` + retry/rate-limit.
- Kiem tra QC co ban:
  - Header `%PDF-`
  - Kich thuoc file >= `min_pdf_bytes`
- Ghi metadata parquet.

Output layout:

- Raw PDF:
  - `data-lake/raw/Semi_Structure_Data/bctc_annual_pdf/source=hnx/date=<run_date>/ticker=<TICKER>/year=<YYYY|UNKNOWN>/<doc_id>.pdf`
- Metadata parquet:
  - `data-lake/raw/Semi_Structure_Data/bctc_annual_pdf_meta/source=hnx/date=<run_date>/PART-000.parquet`
- Bootstrap marker:
  - `data-lake/raw/Semi_Structure_Data/bctc_annual_pdf/source=hnx/_full_bootstrap_done.json`

## Stage 2 - Parse PDF text (no OCR)

- Doc metadata parquet moi nhat (uu tien partition `date=<run_date>`).
- Extract text co ban voi `pdfplumber`.
- Tinh `text_len`.
- Danh dau `needs_ocr=True` neu `text_len < min_text_chars`.

Output layout:

- Text parquet:
  - `data-lake/raw/Semi_Structure_Data/bctc_annual_text/source=hnx/date=<run_date>/PART-000.parquet`

Schema chinh:

- `doc_id`, `ticker`, `year`, `text_len`, `needs_ocr`, `parser_status`, `error`, `ingest_date`

## Cach chay

Trong notebook manager:

- Download only: `run_bctc_annual_pipeline(cfg, include_download=True, include_parse=False)`
- Parse only: `run_bctc_annual_pipeline(cfg, include_download=False, include_parse=True)`
- Full: `run_bctc_annual_pipeline(cfg, include_download=True, include_parse=True)`

## Luu y provider HNX

- **Crawl chinh thuc (tu dong, khong can CSV):** `hnx_disclosure_provider._fetch_hnx_live_api_records` goi POST `www.hnx.vn/ModuleArticles/ArticlesCPEtfs/NextPageTinCPNY_CBTCPH`, phan trang HTML, mo trang chi tiet lay link `.pdf`. Khong dung `hnx_disclosure_api_url` / `HNX_DISCLOSURE_API_URL` cho luong nay (cac field do giu trong config cho tuong lai neu can nguon khac).
  - **Khong can dien tung URL trong JSON:** co the dung **`data/hnx_urls.csv`** (UTF-8, co header). Moi dong mot ban ghi; copy tu Excel/Sheets roi luu CSV.
    - Cot bat buoc: `url_pdf` (hoac `pdf_url`, `url`, `link`).
    - Cot tuy chon: `ticker`, `title`, `published_at`, `url_detail`, `year`.
    - Neu bo trong `title`, he thong tu tao tieu de dang `Bao cao tai chinh nam ... da kiem toan` de qua bo loc BCTC nam.
  - `data/hnx_sample.json` van duoc ho tro (gop voi CSV, loai trung `url_pdf`).
  - Mau CSV: `data/hnx_urls.example.csv` (doi ten thanh `hnx_urls.csv` hoac truyen `SemiStructuredIngestionConfig(hnx_urls_csv=Path(...))`).

## Ghi log tren Windows

- `ingestion.common.configure_logging()` se thu `reconfigure` stream UTF-8 de tranh loi khi log path co dau tieng Viet (vd. thu muc `Đồ Án`).

## SSL / chay thu crawl HNX

- **Mac dinh dev:** `SemiStructuredIngestionConfig.hnx_verify_ssl=False` — khong verify SSL voi `www.hnx.vn` de tranh loi tren Windows. Production: `hnx_verify_ssl=True` hoac `.env` `HNX_SSL_VERIFY=1`.
- Ghi de env: `HNX_SSL_VERIFY=0` / `1`.
- Gioi han so trang khi test: `HNX_CRAWL_MAX_LIST_PAGES=1` (mac dinh toi da 500).

## Loc tieu de BCTC nam

- **Mac dinh:** `strict_bctc_annual_keyword_filter=False` — giu moi PDF tu crawler de ban xem du lieu.
- Bat loc nghiem như trước: `strict_bctc_annual_keyword_filter=True`.

