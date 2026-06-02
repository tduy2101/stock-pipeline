# Luồng dữ liệu BCTC (báo cáo tài chính PDF)

Cập nhật: 2026-06-01

Tài liệu mô tả **toàn bộ** luồng bán cấu trúc BCTC: Bronze (crawl + PDF) → Silver → PostgreSQL `silver` → dbt Gold → FastAPI/React. **Không** có OCR / parse bảng số từ PDF trong scope hiện tại.

```text
HNX (www.hnx.vn) → Bronze (metadata parquet + PDF) → Silver bctc_pdf_meta → silver.bctc_pdf_meta → gold.mart_bctc_documents → API/UI
```

**Tài liệu liên quan:** [Structure](Structure_data_flow.md) · [News](News_data_flow.md) · [README](../README.md)

| Giai đoạn | Code chính | Output |
|---|---|---|
| Bronze | `ingestion/semi_structure_data/`, `ingest_bctc_pdf_manager.ipynb` | `data-lake/raw/Semi_Structure_Data/` |
| Silver | `pipeline/silver/bctc_pdf_meta_transformer.py`, `cli.py` | `data-lake/silver/bctc_pdf_meta/date=*/` |
| Warehouse | `warehouse/loader/silver_loader.py`, `cli.py` | `silver.bctc_pdf_meta`, `silver.load_audit` |
| Gold (dbt) | `stg_bctc_pdf_meta`, `mart_bctc_documents` | Schema `gold` |
| API / UI | `backend/routers/bctc.py`, `BctcPanel.tsx`, `RecentBctcList.tsx` | Theo mã, danh mục, gần đây; stream PDF local |

---

## 1. Bối cảnh dự án (tóm tắt)

Dự án **stock-pipeline** (xem [README.md](../README.md)) là pipeline Medallion cho chứng khoán Việt Nam: Bronze gần nguồn, Silver parquet sạch, PostgreSQL warehouse, dbt Gold, FastAPI read-only, React dashboard.

Luồng BCTC thuộc nhóm **semi-structured data** — một trong ba luồng Bronze (cùng structured OHLCV và news). Bronze **không** load thẳng PostgreSQL.

| Thành phần | Vai trò |
|---|---|
| `ingestion/semi_structure_data/` | Crawl HNX, phân loại tài liệu, tải PDF, ghi metadata parquet |
| `ingestion/ingest_bctc_pdf_manager.ipynb` | Notebook điều phối: cấu hình, chạy pipeline, xem tổng hợp sau download |
| `data-lake/raw/Semi_Structure_Data/` | Bronze root (gitignore) |
| `pipeline/silver/bctc_pdf_meta_transformer.py` | Bronze meta → Silver; dedupe `doc_id`, `display_status` |
| `warehouse/loader/` | Upsert `silver.bctc_pdf_meta` theo `doc_id` |
| `transform/dbt/` | `mart_bctc_documents` cho web |
| `backend/routers/bctc.py` | Đọc Gold + phục vụ file PDF từ đường dẫn Bronze/Silver |

**Ngoài scope:** OCR, extract chỉ tiêu tài chính từ PDF, bảng `silver.bctc_facts`.

---

## 2. Cấu trúc code `ingestion/` (phần BCTC)

```text
ingestion/
├── ingest_bctc_pdf_manager.ipynb     # Điều phối chính (Jupyter)
├── semi_structure_data/
│   ├── __init__.py                   # Export SemiStructuredIngestionConfig, run_bctc_annual_pipeline
│   ├── config.py                     # SemiStructuredIngestionConfig (paths, rate limit, filter, SSL)
│   ├── pipeline.py                   # run_bctc_annual_pipeline (wrapper download)
│   ├── bctc_annual_pdf_ingestor.py   # ingest_bctc_annual_pdfs — crawl + download + ghi parquet
│   ├── document_classifier.py        # Rule-based: doc_class, language, canonical, keep_for_parse
│   ├── downloader.py                 # Stream PDF, resume Range, retry, integrity
│   ├── http_client.py                # Session keep-alive, timeout theo host (owa.hnx.vn)
│   ├── common.py                     # safe_filename, sha256, kiểm tra header %PDF-
│   └── providers/
│       └── hnx_disclosure_provider.py  # Crawl AJAX HNX + sample JSON/CSV
└── common/
    └── __init__.py                   # configure_logging, rate limit, call_with_retry, load .env
```

**Entry point công khai:**

| Hàm | Mô tả |
|---|---|
| `run_bctc_annual_pipeline(cfg, include_download=True)` | Gọi `ingest_bctc_annual_pdfs` — stage duy nhất hiện tại |
| `ingest_bctc_annual_pdfs(cfg)` | Crawl → classify → download → ghi `bctc_annual_pdf_meta` parquet |
| `fetch_hnx_annual_bctc_documents(cfg)` | Chỉ lấy danh sách bản ghi (dùng nội bộ + test) |

Notebook ghi rõ: **chỉ 1 stage** (crawl + download + metadata). Stage parse/OCR đã bỏ.

---

## 3. Nguồn dữ liệu và công cụ (Bronze)

### 3.1. Nguồn chính: Sở Giao dịch Hà Nội (HNX)

| Nguồn | Cách lấy | Endpoint / file |
|---|---|---|
| **Crawl live HNX** (mặc định) | POST AJAX HTML phân trang + POST lấy file đính kèm | `https://www.hnx.vn/ModuleArticles/ArticlesCPEtfs/NextPageTinCPNY_CBTCPH` (danh sách), `.../ArticlesFileAttach` (PDF) |
| **Sample local** (tùy chọn) | Đọc JSON | `data/hnx_sample.json` hoặc `cfg.hnx_sample_json` |
| **CSV URL** (tùy chọn) | Đọc cột `url_pdf`, `ticker`, `title`, … | `data/hnx_urls.csv` hoặc `cfg.hnx_urls_csv` |

`fetch_hnx_annual_bctc_documents` **gộp** theo thứ tự: crawl live → sample JSON → CSV, **loại trùng** theo `url_pdf`.

Filter nhóm tin trên form POST (cố định trong code):

- `pNhomTin`: *Báo cáo tài chính, Giải trình báo cáo tài chính*
- `pTieuDeTin`: *báo cáo tài chính*
- `pMaChungKhoan`: mã CK nếu `cfg.tickers` có **đúng 1** phần tử; rỗng = toàn thị trường

PDF thực tế thường host trên **`owa.hnx.vn`** (link trong HTML đính kèm), không phải chỉ `www.hnx.vn`.

**Không** dùng vnstock cho luồng BCTC. `VNSTOCK_API_KEY` không bắt buộc.

### 3.2. Công cụ và thư viện

| Thành phần | Vai trò |
|---|---|
| **requests** | Session crawl list/detail; session riêng cho batch download (keep-alive) |
| **BeautifulSoup** (`html.parser`) | Parse fragment HTML bảng công bố và danh sách link PDF |
| **pandas** + **pyarrow** / fastparquet | DataFrame metadata → `PART-000.parquet` |
| **ingestion.common** | `wait_for_rate_limit`, `call_with_retry`, `configure_logging`, `load_dotenv_from_project_root` |
| **document_classifier** | Rule-based (không ML): `doc_class`, `language`, `canonical_priority`, `keep_for_parse` |
| **downloader** | Stream `.part` → atomic rename; HTTP Range resume; retry/backoff tự quản |

### 3.3. Biến môi trường quan trọng

| Biến | Tác dụng |
|---|---|
| `HNX_SSL_VERIFY` | `0`/`1` — verify TLS khi gọi `*.hnx.vn` (mặc định code: `hnx_verify_ssl=False` cho dev Windows) |
| `HNX_CRAWL_MAX_LIST_PAGES` | Giới hạn số trang list POST (mặc định code: tối đa **500**; notebook set **100**) |
| `HNX_RESUME_FROM_STATE` | `1` → tiếp crawl từ `last_success_page + 1` trong `_state/hnx_crawl_state.json` |
| `BCTC_INGEST_ALL_CRAWLED_PDFS` | `1` → tải mọi PDF crawl được (tắt filter chỉ BCTC VI) |
| `BCTC_ALLOW_EN_DOCS` | `1` → cho phép tài liệu EN trong filter download / canonical |
| `BCTC_DOWNLOAD_CONNECT_TIMEOUT` / `BCTC_DOWNLOAD_READ_TIMEOUT` / `BCTC_DOWNLOAD_READ_TIMEOUT_HNX_OWA` | Timeout tải PDF (read dài hơn cho `owa.hnx.vn`, mặc định 180s) |
| `HNX_DISCLOSURE_API_URL` | Khai báo trong config nhưng **crawl hiện tại không dùng** — endpoint cố định trên `www.hnx.vn` |

---

## 4. Điều phối chạy (notebook & CLI)

### 4.1. Notebook `ingest_bctc_pdf_manager.ipynb`

Luồng cell:

1. **Setup:** UTF-8 console, `sys.path` → repo root, tắt warning Unicode trên Windows.
2. **Giới hạn crawl test:** `HNX_CRAWL_MAX_LIST_PAGES=100` (cell riêng).
3. **Cấu hình:** `configure_logging()`, `load_dotenv_from_project_root()`, tạo `SemiStructuredIngestionConfig`:

   | Tham số | Giá trị notebook | Ý nghĩa |
   |---|---|---|
   | `sources` | `["hnx"]` | Nguồn duy nhất phase hiện tại |
   | `incremental_window_days` | `30` | **Chưa được dùng** trong code crawl (dự phòng / đồng bộ tên với structured) |
   | `rate_limit_rpm` | `20` | Giãn cách request crawl + download |
   | `min_pdf_bytes` | `20_000` | QC: file nhỏ hơn coi là fail |
   | `hnx_verify_ssl` | mặc định `False` | Crawl được trên Windows dev |
   | `strict_bctc_annual_keyword_filter` | mặc định `False` | Giữ mọi PDF BCTC (cả quý), không bắt buộc hint “năm/kiểm toán” |

4. **Chạy:** `out_download = run_bctc_annual_pipeline(cfg, include_download=True)`.
5. **Kiểm tra:** đọc parquet metadata, group theo `ticker`, đếm `status`.

**Mặc định notebook** ưu tiên **xem đủ dữ liệu** khi dev. Production nên: `HNX_SSL_VERIFY=1`, `hnx_verify_ssl=True`, có thể `strict_bctc_annual_keyword_filter=True` để siết BCTC năm.

### 4.2. Chạy từ Python (không notebook)

```powershell
@'
from ingestion.semi_structure_data import SemiStructuredIngestionConfig, run_bctc_annual_pipeline

cfg = SemiStructuredIngestionConfig()
print(run_bctc_annual_pipeline(cfg, include_download=True))
'@ | python -
```

`run_date` = `cfg.run_partition` nếu set, ngược lại **`date.today().isoformat()`** — dùng làm partition Bronze `date=<run_date>` và thư mục PDF, **không** phải ngày `published_at` trên HNX.

---

## 5. Cơ chế lấy dữ liệu (Bronze)

### 5.1. Kiến trúc một stage

```mermaid
flowchart TB
  subgraph sources [Nguồn]
    HNX[List POST www.hnx.vn]
    JSON[hnx_sample.json]
    CSV[hnx_urls.csv]
  end
  subgraph crawl [fetch_hnx_annual_bctc_documents]
    P[Parse HTML rows]
    A[POST ArticlesFileAttach]
    F[Keyword filter attachment]
    D[Dedupe url_pdf]
    C[classify + canonical]
  end
  subgraph ingest [ingest_bctc_annual_pdfs]
    DL[download_pdf_to_path]
    QC[QC header + size + sha256]
    META[Ghi metadata parquet]
    MK[Bootstrap marker tùy chọn]
  end
  HNX --> P --> A --> F
  JSON --> D
  CSV --> D
  F --> D --> C --> DL --> QC --> META --> MK
  DL --> PDF["bctc_annual_pdf/.../*.pdf"]
  META --> PARQ["bctc_annual_pdf_meta/.../PART-000.parquet"]
```

### 5.2. Crawl danh sách HNX (`_fetch_hnx_live_api_records`)

1. Tạo `requests.Session` với header AJAX (`X-Requested-With`, `Referer`, form-urlencoded).
2. Vòng **trang** `pNumPage = 1 … max_pages`:
   - `wait_for_rate_limit` + `call_with_retry` POST list → HTML fragment.
   - `_parse_hnx_list_rows`: mỗi `<tr>` lấy `ticker`, `title`, `published_at`, `article_id` (onclick `ViewDetailArticlesByID` / `ShowFileAttach`).
   - Trang rỗng → dừng (hết dữ liệu).
3. Với mỗi `article_id` (cache theo id): POST `ArticlesFileAttach` → parse link `.pdf`.
4. Lọc attachment:
   - **`strict_bctc_annual_keyword_filter=False`** (mặc định): `_is_bctc_candidate` — phải có từ khóa BCTC, loại `uponrequest`, `nghi quyet`, token `EN` trong haystack chuẩn hóa.
   - **`strict_bctc_annual_keyword_filter=True`**: thêm hint năm / kiểm toán / hợp nhất (`_is_annual_bctc_candidate`).
5. Gán `year` bằng regex `20xx` từ title / ngày đăng.
6. Nếu `HNX_RESUME_FROM_STATE=1`: sau mỗi trang thành công, ghi `data-lake/raw/Semi_Structure_Data/_state/hnx_crawl_state.json` (`last_success_page`).

**Lưu ý:** Mỗi lần chạy mặc định **quét lại từ trang 1** (full list theo số trang cap), trừ khi bật resume state sau khi crawl bị ngắt giữa chừng.

### 5.3. Phân loại và chọn bản canonical (`document_classifier.py`)

Trên **toàn batch** sau crawl:

- Chuẩn hóa title (bỏ dấu, lowercase).
- `doc_class`: `financial_statement_consolidated` | `financial_statement_separate` | `explanation` | `disclosure` | `announcement` | `unknown`.
- `language`: `VI` | `EN` | `UNKNOWN` (heuristic tên file `_VI_` / `_EN_`, tiếng Việt có dấu).
- `period_key`: ví dụ `2024-ANNUAL`, `2024-Q1` — nhóm dedupe canonical theo `(ticker, period_key)`.
- `apply_canonical_selection`: mỗi nhóm giữ **một** bản `keep_for_parse=True` (ưu tiên consolidated VI, priority nhỏ hơn).

`doc_id` ổn định:

```text
sha1( source | ticker | title | published_at | url_pdf )
```

### 5.4. Quyết định tải PDF (`should_download_financial_pdf`)

Khi `ingest_only_financial_statement_vi=True` (mặc định, trừ `BCTC_INGEST_ALL_CRAWLED_PDFS=1`):

| Điều kiện | Hành vi |
|---|---|
| `doc_class` là BCTC hợp nhất / riêng + `language=VI` | **Tải** |
| `language=UNKNOWN` và `ingest_unknown_language_financial=True` | **Tải** |
| EN (không bật `BCTC_ALLOW_EN_DOCS`) | **Bỏ qua** — metadata vẫn ghi, `status=skipped_ingest_filter` |
| Giải trình, CBTT, announcement, unknown class | **Bỏ qua** tải |

Mọi bản ghi crawl đều có dòng metadata (kể cả skip/fail).

### 5.5. Tải file (`downloader.py`)

- Stream vào `{doc_id}.pdf.part`, kiểm tra `%PDF-`, `Content-Length`, `%%EOF` (best-effort).
- Resume: header `Range: bytes=<size>-` nếu `.part` còn dời dang.
- Retry riêng: `download_retry_max_attempts` (mặc định 3), backoff exponential.
- Thành công → `os.replace` → `{doc_id}.pdf` dưới partition `date=run_date`.

### 5.6. Lần đầu (backfill) vs lần sau (incremental)

Khác luồng structured OHLCV, BCTC Bronze **chưa** có watermark theo ngày đăng hay `pFromDate`/`pToDate` trên API HNX.

| Khái niệm | Hành vi thực tế trong code |
|---|---|
| **Lần đầu / backfill** | Crawl tối đa N trang list (500 hoặc env/notebook), tải PDF thỏa filter, ghi partition `date=<run_date>`. Nếu `bootstrap_full_history_if_missing=True` và chưa có file `bctc_annual_pdf/source=hnx/_full_bootstrap_done.json` → sau run tạo **marker** bootstrap (chỉ ghi nhận, không đổi phạm vi crawl). |
| **Lần chạy tiếp theo** | **Cùng cơ chế full crawl** theo số trang (không đọc `incremental_window_days`). Metadata mới nằm partition **`date=` ngày chạy mới**. Upsert warehouse/Silver theo `doc_id` khi load — trùng `doc_id` thì cập nhật. |
| **Crawl bị gián đoạn** | `HNX_RESUME_FROM_STATE=1` + file `_state/hnx_crawl_state.json` → tiếp từ trang kế tiếp **trong cùng một run**, không phải incremental theo lịch. |
| **`full_bootstrap_once_then_incremental=True`** (mặc định `False`) | Sau khi có marker, `_source_needs_bootstrap` trả `False` — **vẫn chưa** giới hạn crawl theo cửa sổ ngày (chưa implement). |
| **`cfg.tickers`** | Nếu set nhiều mã: lọc sau crawl; một mã duy nhất: truyền vào POST list HNX. |

**Kết luận vận hành:** Coi mỗi job Bronze BCTC là **snapshot crawl + download theo ngày chạy** (`run_date`). “Incremental” ở tầng warehouse là **upsert theo `doc_id`**, không phải crawl chỉ bài mới. Trường `incremental_window_days` trong config/notebook **chưa nối** vào provider.

Ví dụ output notebook (workspace demo 2026-05-14):

- `documents_crawled`: 1458  
- `download_ok`: 952  
- `download_skipped_filter`: 506  
- `download_fail`: 0  

---

## 6. Output Bronze

### 6.1. Đường dẫn

Root: `data-lake/raw/Semi_Structure_Data/` (từ `SemiStructuredIngestionConfig.data_lake_root`).

```text
# Metadata — một file parquet mỗi run (partition theo ngày chạy job)
bctc_annual_pdf_meta/source=hnx/date=<YYYY-MM-DD>/PART-000.parquet

# File PDF — partition theo ngày chạy job + ticker + năm báo cáo
bctc_annual_pdf/source=hnx/date=<YYYY-MM-DD>/ticker=<TICKER>/year=<YYYY>/<doc_id>.pdf

# Trạng thái crawl (tùy chọn)
_state/hnx_crawl_state.json

# Đánh dấu đã chạy bootstrap ít nhất một lần (theo source)
bctc_annual_pdf/source=hnx/_full_bootstrap_done.json
```

File tạm khi đang tải: `<doc_id>.pdf.part` (cùng thư mục; có thể giữ lại nếu lỗi mạng để resume).

### 6.2. Schema metadata parquet (`PART-000`)

Mỗi dòng = một tài liệu PDF (đã crawl), kể cả không tải được.

| Nhóm | Cột | Mô tả |
|---|---|---|
| **Định danh** | `doc_id` | SHA-1 ổn định (xem 5.3) |
| | `source` | `hnx` |
| | `ticker`, `year` | Mã CK, năm suy ra |
| **Nguồn HNX** | `title`, `published_at` | Tiêu đề hiệu lực, ngày công bố (chuỗi từ bảng HNX) |
| | `url_pdf`, `url_detail` | Link tải / trang công bố |
| **Lưu trữ** | `ingest_date` | = `run_date` |
| | `pdf_path` | Đường dẫn đích PDF trên disk |
| | `file_size`, `sha256` | Sau tải thành công + QC |
| **QC / trạng thái** | `pdf_valid_header`, `qc_pass` | Header `%PDF-` và size ≥ `min_pdf_bytes` |
| | `status` | `downloaded` \| `skipped_ingest_filter` \| `qc_failed` \| `download_failed` |
| | `error`, `error_class` | Chi tiết lỗi hoặc lý do skip |
| **HTTP download** | `http_status`, `content_length`, `bytes_downloaded`, `download_seconds`, `attempts`, `integrity_ok` | Từ `DownloadResult` |
| **Phân loại** | `normalized_title`, `doc_class`, `language` | Rule-based |
| | `is_consolidated`, `is_explanation`, `is_disclosure` | Cờ loại tài liệu |
| | `canonical_priority`, `keep_for_parse` | Chọn bản “chính” cho tương lai parse / Gold |

### 6.3. Grain và idempotency Bronze

| Đối tượng | Grain / ghi đè |
|---|---|
| Metadata partition | Một `PART-000.parquet` / `source` / `date=<run_date>` — **snapshot** toàn bộ kết quả crawl của run đó |
| PDF file | `{doc_id}.pdf` — rerun cùng `run_date` + cùng `doc_id` **ghi đè** file |
| Dedupe crawl | Trùng `url_pdf` khi gộp nguồn |

---

## 7. Silver — Bronze → parquet sạch (`pipeline/silver/`)

### 7.1. Cấu trúc code liên quan

```text
pipeline/silver/
├── config.py                      # SilverConfig — đường dẫn Bronze/Silver, bctc_pdf_meta_bronze_path()
├── cli.py                         # --dataset bctc_pdf_meta --run-partition <YYYY-MM-DD>
├── bronze_reader.py               # SilverWriteResult, write_single_part_parquet()
├── runs_log.py                    # Ghi _runs.jsonl sau mỗi transform
├── text_utils.py                  # normalize_text, coerce_bool, upper/lower ticker
└── bctc_pdf_meta_transformer.py   # transform_bctc_pdf_meta, run_bctc_pdf_meta_silver
```

**Entry point:** `run_bctc_pdf_meta_silver(cfg, run_partition=..., source="hnx")`.

**Lưu ý CLI:**

- `--run-partition` **bắt buộc** cho `bctc_pdf_meta` (phải trùng `date=` partition Bronze, ví dụ `2026-05-14`).
- `--strict` chỉ áp dụng dataset `news`, **không** ảnh hưởng BCTC.
- `python -m pipeline.silver.cli --dataset all` chỉ chạy **structured** (`price` … `price_board`); **không** gồm `bctc_pdf_meta` hay `news` — phải gọi riêng.

```powershell
python -m pipeline.silver.cli --dataset bctc_pdf_meta --run-partition 2026-05-14
```

### 7.2. Đầu vào / đầu ra

| | Đường dẫn |
|---|---|
| **Đọc Bronze** | `data-lake/raw/Semi_Structure_Data/bctc_annual_pdf_meta/source=hnx/date=<run_partition>/PART-000.parquet` |
| **Ghi Silver** | `data-lake/silver/bctc_pdf_meta/date=<run_partition>/PART-000.parquet` |
| **Audit** | `data-lake/silver/bctc_pdf_meta/_runs.jsonl` |

`SilverConfig.bctc_pdf_meta_bronze_path(run_partition, source="hnx")` resolve đúng path trên. Nếu thiếu file Bronze → `FileNotFoundError`.

Cột Bronze **không** đưa vào Silver (bỏ ở ingest, không nằm `BCTC_INPUT_COLUMNS`): `ingest_date`, `http_status`, `content_length`, `bytes_downloaded`, `download_seconds`, `attempts`, `integrity_ok`, `error_class`.

### 7.3. Luồng xử lý (`transform_bctc_pdf_meta`)

```mermaid
flowchart LR
  B[Bronze PART-000.parquet]
  R[Đọc + bổ sung cột thiếu]
  N[Chuẩn hóa dtype / text]
  D[Dedupe doc_id]
  E[period_key + display_status]
  W[write_single_part_parquet]
  L[_runs.jsonl]
  B --> R --> N --> D --> E --> W --> L
```

**Bước chi tiết:**

1. **Đọc** parquet Bronze; thêm cột NA nếu thiếu so với `BCTC_INPUT_COLUMNS`; gắn `source_file`, `run_partition`.
2. **Chuẩn hóa:**
   - `doc_id` — text, bỏ dòng rỗng (DQ warning).
   - `source` — lowercase; `ticker` — uppercase.
   - `year`, `file_size`, `canonical_priority` — numeric nullable.
   - `published_at` — `datetime64 UTC`, `dayfirst=True` (phù hợp `dd/mm/yyyy` HNX).
   - Cột bool: `pdf_valid_header`, `qc_pass`, `is_*`, `keep_for_parse`.
3. **Dedupe grain `doc_id`** (trong một partition Bronze):
   - Sort: `downloaded` trước → `qc_pass` cao → `file_size` lớn → `keep="first"`.
   - Ghi warning nếu có bản trùng.
4. **Suy luận thêm (Silver-only):**
   - **`period_key`:** dùng `period_key` Bronze nếu có; không thì parse từ `title` / `normalized_title` / `url_pdf` → `Q1`…`Q4`, `H1`, `9M`, `ANNUAL`, `GENERAL`.
   - **`display_status`:** `available` nếu `status=downloaded` + `qc_pass` + `file_size>0` + có `pdf_path` hoặc `url_pdf`; `skipped*` → `skipped`; chứa `failed` hoặc `!qc_pass` → `failed`; còn lại `unknown`.
   - **`is_available_for_web`:** `display_status == available` và có đường dẫn/URL.
5. **`silver_loaded_at`:** timestamp UTC lúc transform.
6. **Ghi** `PART-000.parquet` (xóa `PART-*.parquet` cũ trong cùng thư mục partition trước khi ghi).

`run_bctc_pdf_meta_silver` bọc transform + ghi file + `write_runs_entry` (`status=success|error`). **Không** cập nhật `_watermark.json` (khác OHLCV).

### 7.4. Schema Silver output (`BCTC_OUTPUT_COLUMNS`)

| Cột | Nguồn / ý nghĩa |
|---|---|
| `doc_id`, `source`, `ticker`, `year` | Từ Bronze, đã chuẩn hóa |
| `period_key` | Bronze hoặc `_infer_period_key` |
| `title`, `normalized_title`, `published_at` | Metadata công bố |
| `url_pdf`, `url_detail`, `pdf_path` | Link + file local |
| `file_size`, `sha256`, `pdf_valid_header`, `qc_pass` | QC file |
| `status`, `error` | Trạng thái ingest/download |
| `doc_class`, `language`, `is_consolidated`, `is_explanation`, `is_disclosure` | Classification |
| `canonical_priority`, `keep_for_parse` | Chọn bản canonical (parse tương lai) |
| `display_status`, `is_available_for_web` | **Mới ở Silver** — phục vụ web/Gold |
| `run_partition` | = tham số CLI (ngày job) |
| `source_file` | Path parquet Bronze đã đọc |
| `silver_loaded_at` | Thời điểm transform |

### 7.5. Grain & partition Silver

| Khái niệm | Quy tắc |
|---|---|
| **Grain logic** | `doc_id` (một dòng / tài liệu PDF) |
| **Partition filesystem** | `date=<run_partition>` — **theo ngày chạy job**, không theo `published_at` |
| **Incremental Silver** | Không tự đọc partition cũ khi transform; mỗi lần chỉ xử lý **một** `run_partition` chỉ định. Nhiều ngày chạy → nhiều thư mục `date=*` cùng tồn tại |
| **Dedupe cross-partition** | Xảy ra ở **warehouse load** (xem mục 8), không trong một lần transform đơn |

Mỗi dòng `_runs.jsonl`: `run_id`, `dataset`, `run_partition`, `started_at`, `finished_at`, `input_rows`, `output_rows`, `latest_key`, `status`, `error`.

---

## 8. Warehouse — Silver parquet → PostgreSQL

### 8.1. CLI

```powershell
$env:DATABASE_URL = "postgresql://stock:stock@localhost:55432/stock_pipeline"
python -m warehouse.loader.cli load-silver --dataset bctc_pdf_meta
# hoặc
python -m warehouse.loader.cli load-silver --dataset all   # bctc_pdf_meta là dataset cuối
```

Thứ tự `all`: `price` → … → `news` → **`bctc_pdf_meta`**.

Cần DDL: `warehouse/ddl/schema.sql` (bảng `silver.bctc_pdf_meta`, index unique `url_pdf`).

### 8.2. Cơ chế load (`silver_loader.py`)

1. **Đọc:** `glob` recursive `data-lake/silver/bctc_pdf_meta/**/*.parquet` — **gộp mọi partition** `date=*` thành một DataFrame.
2. **`prepare_dataframe`:**
   - Chỉ giữ cột whitelist (29 cột, khớp Silver output).
   - Ép `run_partition` → **date**; `published_at`, `silver_loaded_at` → timestamptz; bool/text theo config.
3. **`_dedupe_for_load`** (`dedupe_on_load=True`):
   - Sort: `run_partition`, `silver_loaded_at`, `published_at` (tăng dần).
   - `drop_duplicates(doc_id, keep="last")` — partition/run **mới hơn** thắng khi trùng `doc_id`.
4. **Validate:** `doc_id` not null, không trùng key sau dedupe; `url_pdf` unique trong batch (non-null).
5. **Upsert:** `INSERT INTO silver.bctc_pdf_meta ... ON CONFLICT (doc_id) DO UPDATE` (batch 2000); đếm insert/update.
6. **Audit:** `silver.load_audit` (`dataset`, `run_partition` = max trong batch, `rows_read`, `rows_inserted`, `rows_updated`, `status`).

Load **idempotent** theo `doc_id`: chạy lại sau khi thêm partition Silver mới → cập nhật bản ghi cũ, không nhân đôi.

### 8.3. Bảng `silver.bctc_pdf_meta`

| | |
|---|---|
| **Primary key** | `doc_id` |
| **Unique phụ** | `url_pdf` (WHERE NOT NULL) |
| **Cột audit DB** | `loaded_at` (default `now()`, do loader set khi insert) |

Các cột business map 1-1 từ Silver parquet (không có cột HTTP download Bronze).

---

## 9. Gold — dbt Silver → analytics (`transform/dbt/`)

dbt đọc **bảng PostgreSQL `silver.bctc_pdf_meta`** (sau `load-silver`), build object trong schema **`gold`** (`profiles.yml`: `schema: gold`). **Không** đọc parquet Silver trên disk.

Luồng BCTC trong dbt **rất mỏng**: không có model `intermediate` — chỉ **staging ephemeral** → **mart table**. Không join `fact_price_daily`, `dim_company`, hay mart khác.

### 9.1. Cấu trúc project (file liên quan BCTC)

```text
transform/dbt/
├── dbt_project.yml              # staging=view (mặc định), marts=table
├── profiles.yml                 # Postgres :55432, schema gold
└── models/
    ├── staging/
    │   ├── sources.yml          # source('silver', 'bctc_pdf_meta') + cột khai báo
    │   ├── schema.yml           # tests: doc_id unique, ticker not_null
    │   └── stg_bctc_pdf_meta.sql   # config materialized='ephemeral'
    └── marts/
        ├── mart_bctc_documents.sql  # config materialized='table'
        └── schema.yml               # tests mart: doc_id unique, ticker not_null
```

Launcher từ **repo root** (`dbt_project.yml` gốc trỏ `transform/dbt/models`):

```powershell
dbt debug --profiles-dir transform/dbt
dbt run --profiles-dir transform/dbt
dbt test --profiles-dir transform/dbt

# Chỉ build nhánh BCTC
dbt run --profiles-dir transform/dbt --select stg_bctc_pdf_meta+mart_bctc_documents
dbt test --profiles-dir transform/dbt --select stg_bctc_pdf_meta mart_bctc_documents
```

| Lệnh | Ý nghĩa |
|---|---|
| `dbt run` (full) | Build **toàn bộ** DAG (price, news, BCTC, …) nếu đủ bảng `silver.*` |
| `--select stg_bctc_pdf_meta+mart_bctc_documents` | Chỉ upstream + mart BCTC |
| `dbt test` | Chạy tests trong `staging/schema.yml` và `marts/schema.yml` cho các model được build |

**Điều kiện tiên quyết:** `warehouse/ddl/schema.sql` đã apply; `python -m warehouse.loader.cli load-silver --dataset bctc_pdf_meta` đã chạy thành công.

**Lưu ý:** `gold.mart_bctc_documents` **không** được tạo sẵn trong `schema.sql` — bảng vật lý do **dbt** `CREATE TABLE` khi `dbt run` (materialized `table`).

### 9.2. Source — `silver.bctc_pdf_meta`

Khai báo `models/staging/sources.yml`:

```yaml
sources:
  - name: silver
    schema: silver
    tables:
      - name: bctc_pdf_meta
        columns: [doc_id, ticker, year, period_key, title, published_at,
                  url_pdf, pdf_path, file_size, doc_class, is_consolidated,
                  display_status, is_available_for_web, qc_pass, status]
```

Bảng Silver đầy đủ hơn (loader whitelist ~29 cột): gồm `normalized_title`, `url_detail`, `sha256`, `status`, `error`, `language`, `keep_for_parse`, `run_partition`, `source_file`, `silver_loaded_at`, `loaded_at`, … — **dbt staging chỉ SELECT subset** phục vụ web; cột còn lại vẫn có trong `silver.bctc_pdf_meta` cho tra cứu SQL/ad-hoc.

| Grain Silver | `doc_id` (PK) |
|---|---|
| Unique phụ | `url_pdf` (partial index, NOT NULL) |
| Upsert loader | `ON CONFLICT (doc_id) DO UPDATE` |

### 9.3. Staging — `stg_bctc_pdf_meta` (ephemeral)

File: `models/staging/stg_bctc_pdf_meta.sql`

```sql
{{ config(materialized='ephemeral') }}

select
  doc_id,
  ticker,
  year,
  period_key,
  title,
  published_at,
  url_pdf,
  pdf_path,
  file_size,
  doc_class,
  is_consolidated,
  display_status,
  is_available_for_web
from {{ source('silver', 'bctc_pdf_meta') }}
where display_status != 'error'
  and is_available_for_web = true
  and doc_id is not null
```

| Đặc điểm | Chi tiết |
|---|---|
| **Materialization** | **`ephemeral`** — ghi đè mặc định folder `staging: view`; **không** tạo `gold.stg_bctc_pdf_meta`; SQL được inline vào `mart_bctc_documents` |
| **Grain** | **`doc_id`** (1 dòng / tài liệu PDF, sau lọc) |
| **Lọc `is_available_for_web`** | Chỉ bản Silver đã `display_status=available` (download + QC + có path/URL) — loại `skipped`, `failed`, `unknown` |
| **Lọc `display_status != 'error'`** | Phòng giá trị lạ; Silver hiện tại chỉ emit `available` \| `skipped` \| `failed` \| `unknown` (không có `error`) |
| **Không aggregate** | Không gom theo `ticker+year`; mỗi PDF một dòng |

**dbt tests** (`staging/schema.yml`): `doc_id` — `not_null`, `unique`; `ticker` — `not_null`.

### 9.4. Mart — `mart_bctc_documents` (table)

File: `models/marts/mart_bctc_documents.sql`

```sql
{{ config(materialized='table') }}

select
  doc_id,
  ticker,
  year,
  period_key,
  title,
  published_at,
  doc_class,
  is_consolidated,
  display_status,
  is_available_for_web,
  url_pdf,
  pdf_path,
  file_size
from {{ ref('stg_bctc_pdf_meta') }}
```

| Đặc điểm | Chi tiết |
|---|---|
| **Materialization** | **`table`** → vật lý **`gold.mart_bctc_documents`** |
| **Transform** | **Pass-through** từ staging — không cast thêm, không dedupe, không join |
| **Grain** | **`doc_id`** — output analytics/API chính cho luồng BCTC |
| **Cột đặc biệt** | `pdf_path` — có trong mart để endpoint **file** stream local; **không** trả trong `GET /bctc/{symbol}` list |

**dbt tests** (`marts/schema.yml`): `doc_id` — `not_null`, `unique`; `ticker` — `not_null`.

### 9.5. Lineage cột (Silver → Gold → API)

| Cột | `silver.bctc_pdf_meta` | `stg_bctc_pdf_meta` | `mart_bctc_documents` | `GET /bctc/{symbol}` | `GET .../file/{doc_id}` |
|---|---|:---:|:---:|:---:|:---:|
| `doc_id` | ✓ | ✓ | ✓ | ✓ | ✓ (lookup) |
| `ticker` | ✓ | ✓ | ✓ | ✓ | ✓ |
| `year` | ✓ | ✓ | ✓ | ✓ | — |
| `period_key` | ✓ | ✓ | ✓ | ✓ | — |
| `title` | ✓ | ✓ | ✓ | ✓ | ✓ (log) |
| `published_at` | ✓ | ✓ | ✓ | ✓ | — |
| `doc_class` | ✓ | ✓ | ✓ | ✓ | — |
| `is_consolidated` | ✓ | ✓ | ✓ | ✓ | — |
| `display_status` | ✓ | ✓ | ✓ | ✓ | — |
| `is_available_for_web` | ✓ | ✓ | ✓ | (lọc SQL) | ✓ (403 nếu false) |
| `url_pdf` | ✓ | ✓ | ✓ | ✓ | fallback redirect |
| `pdf_path` | ✓ | ✓ | ✓ | — | ✓ `FileResponse` |
| `file_size` | ✓ | ✓ | ✓ | ✓ | — |
| `normalized_title`, `sha256`, `status`, `error`, … | ✓ | — | — | — | — |

### 9.6. Phễu lọc dữ liệu & snapshot demo

```text
Bronze crawl (một run)     →  ~1458 dòng metadata (mọi status)
Silver transform           →  cùng partition, thêm display_status / is_available_for_web
silver.bctc_pdf_meta (PG)  →  ~1458 rows (toàn bộ lịch sử load, dedupe doc_id)
stg + mart (lọc web)       →  ~952 rows   (chỉ is_available_for_web = true)
```

| Layer | Rows (demo README 2026-06-01) | Ghi chú |
|---|---:|---|
| `silver.bctc_pdf_meta` | 1,458 | Mọi tài liệu đã crawl (kể cả skip/fail) |
| **`gold.mart_bctc_documents`** | **952** | PDF tải OK + QC + hiển thị web |

Chênh lệch ~506 dòng ≈ `download_skipped_filter` + failed/skipped ở Bronze (không đạt `is_available_for_web`).

### 9.7. DAG phụ thuộc (BCTC)

```mermaid
flowchart TB
  subgraph pg_silver [PostgreSQL silver]
    sb[silver.bctc_pdf_meta]
  end
  subgraph dbt_gold [dbt → schema gold]
    stg[stg_bctc_pdf_meta ephemeral]
    mart[mart_bctc_documents table]
  end
  subgraph app [Ứng dụng]
    api[List + File API]
    ui[BctcPanel React]
  end
  disk[(PDF files pdf_path)]

  sb --> stg --> mart
  mart --> api --> ui
  disk -.->|FileResponse| api
```

**Không có:**

- `int_bctc_*` hoặc aggregate `ticker + year`
- Liên kết `mart_stock_daily` / `mart_company_profile`
- Model Gold lưu số liệu tài chính trích từ PDF (`silver.bctc_facts` — commented out trong DDL, out of scope)

### 9.8. So sánh với luồng news (dbt)

| | News | BCTC |
|---|---|---|
| Staging | `stg_news` ephemeral | `stg_bctc_pdf_meta` ephemeral |
| Intermediate | `int_news_sentiment_daily` (aggregate) | **Không** |
| Mart chính | `mart_stock_news_daily` (`ticker + published_date`) | `mart_bctc_documents` (`doc_id`) |
| Grain API | Tổng hợp theo ngày | **Từng tài liệu PDF** |
| Ad-hoc chi tiết | Query `silver.news` | Query `silver.bctc_pdf_meta` |

### 9.9. Output cuối cùng — đối tượng Gold & Silver

| Object | Loại | Grain | Mục đích |
|---|---|---|---|
| `silver.bctc_pdf_meta` | table (PG) | `doc_id` | Warehouse đầy đủ metadata + QC + classification |
| `stg_bctc_pdf_meta` | ephemeral (CTE) | `doc_id` | Lọc bản ghi web-ready |
| **`gold.mart_bctc_documents`** | **table** | **`doc_id`** | **Mart duy nhất** cho API/UI — danh sách PDF theo mã |

File PDF **không** nằm trong DB — chỉ đường dẫn `pdf_path` trỏ tới `data-lake/raw/.../bctc_annual_pdf/`.

---

## 10. API & Frontend (output ứng dụng)

### 10.1. FastAPI

Router: `backend/routers/bctc.py` — prefix `/bctc`, tag `bctc`, đăng ký trong `backend/main.py`.

Backend **chỉ đọc schema `gold`** (`mart_bctc_documents`), không query `silver.bctc_pdf_meta` hay đọc parquet.

#### `GET /bctc/documents?ticker=&year=&q=&page=&page_size=`

| | |
|---|---|
| **Bảng** | `gold.mart_bctc_documents` |
| **Lọc** | `is_available_for_web = true`; `ticker`, `year`, `title ILIKE q` (tùy chọn) |
| **Response** | `PaginatedResponse[BctcDocumentRow]` — tra cứu **toàn thị trường** / lọc mã |

#### `GET /bctc/recent?page_size=`

| | |
|---|---|
| **Bảng** | `gold.mart_bctc_documents` |
| **Sort** | `published_at DESC`, `year DESC` |
| **Response** | `list[BctcDocumentRow]` — widget dashboard (`RecentBctcList`) |

#### `GET /bctc/{symbol}?year=`

| | |
|---|---|
| **Bảng** | `gold.mart_bctc_documents` |
| **Lọc** | `ticker = UPPER(symbol)`; `is_available_for_web = true`; `year` optional |
| **Sort** | `year DESC NULLS LAST`, `published_at DESC NULLS LAST` |
| **404** | Không có dòng → `"Khong tim thay tai lieu BCTC cho ticker: …"` |

**Response** — `list[BctcDocumentRow]` (`backend/schemas/bctc.py`):

| Field | Kiểu | Nguồn mart |
|---|---|---|
| `doc_id` | str | ✓ |
| `ticker` | str | ✓ |
| `year` | int \| null | ✓ |
| `period_key` | str \| null | ✓ (Q1…Q4, ANNUAL, GENERAL, …) |
| `title` | str \| null | ✓ |
| `published_at` | datetime \| null | ✓ |
| `doc_class` | str \| null | ví dụ `financial_statement_consolidated` |
| `is_consolidated` | bool \| null | ✓ |
| `display_status` | str \| null | thường `available` sau lọc |
| `is_available_for_web` | bool \| null | ✓ |
| `url_pdf` | str \| null | link HNX/owa |
| `file_size` | int \| null | bytes |

**Không trả:** `pdf_path`, `url_detail`, `sha256`, `keep_for_parse`, …

#### `GET /bctc/{symbol}/file/{doc_id}`

| Bước | Hành vi |
|---|---|
| Lookup | `doc_id` + `ticker` trên `gold.mart_bctc_documents` |
| 404 | Không tìm thấy cặp mã/tài liệu |
| 403 | `is_available_for_web = false` |
| 200 inline PDF | `pdf_path` tồn tại trên disk (absolute hoặc relative `Path.cwd()`) → `FileResponse` `application/pdf` |
| 302 redirect | Không có file local → `RedirectResponse` tới `url_pdf` |
| 404 cuối | Không file và không URL |

Ví dụ smoke test:

```powershell
curl "http://localhost:8000/bctc/documents?page_size=20"
curl "http://localhost:8000/bctc/recent?page_size=10"
curl http://localhost:8000/bctc/AAV
curl "http://localhost:8000/bctc/AAV?year=2024"
curl -I "http://localhost:8000/bctc/AAV/file/<doc_id>"
```

### 10.2. React / Vite

| Thành phần | Vai trò |
|---|---|
| `frontend/src/api/bctc.ts` | `fetchBctcDocuments`, `fetchRecentBctc`, `fetchAllBctcDocuments`; `getBctcFileUrl` |
| `frontend/src/hooks/useBctc.ts` | Query theo symbol/năm |
| `frontend/src/components/stock/BctcPanel.tsx` | Tab BCTC trên `/stock/:symbol` |
| `frontend/src/components/market/RecentBctcList.tsx` | Dashboard — `GET /bctc/recent` |
| `frontend/src/pages/StockDetailPage.tsx` | Tab `bctc` |
| `frontend/src/types/index.ts` | `BctcDocumentRow` — khớp API list (không `pdf_path`) |

**UI hành vi (`BctcPanel`):**

- Gọi `useBctc(symbol)` để lấy danh sách năm từ `year` trên mọi doc.
- Filter client: chọn năm → gọi lại API với `?year=`.
- Mỗi card: `title`, `year` / `period_key`, `doc_class`, `file_size`, `published_at`.
- **Xem PDF:** `window.open(getBctcFileUrl(...))` — trỏ proxy dev `/api` hoặc `VITE_API_URL`.

### 10.3. Luồng người dùng (output cuối)

```mermaid
sequenceDiagram
  participant U as User
  participant UI as BctcPanel
  participant API as FastAPI
  participant G as gold.mart_bctc_documents
  participant D as Disk PDF

  U->>UI: Mở tab BCTC /stock/VCB
  UI->>API: GET /bctc/VCB
  API->>G: SELECT ... is_available_for_web
  G-->>API: rows
  API-->>UI: JSON list
  U->>UI: Click Xem PDF
  UI->>API: GET /bctc/VCB/file/doc_id
  API->>G: pdf_path, url_pdf
  alt file local
    API->>D: read pdf_path
    API-->>U: application/pdf inline
  else redirect
    API-->>U: 302 url_pdf
  end
```

### 10.4. Phạm vi MVP (chưa có)

- Không API/dbt expose chỉ tiêu tài chính parse từ PDF.
- Không `silver.bctc_facts` / OCR pipeline.
- Không full-text search trong PDF trên UI.
- Không upload/sync PDF lên object storage — path local phụ thuộc máy chạy ingestion.

---

## 11. So sánh nhanh với luồng structured

| | Structured (vnstock) | BCTC PDF |
|---|---|---|
| Nguồn | vnstock API | HNX web + PDF host |
| Partition Bronze chính | Tháng giao dịch / snapshot | **`date=run_date`** job |
| Incremental crawl | Watermark OHLCV | Full list pages (resume state tùy chọn) |
| Silver partition | `trading_date` / `current` / `period_type` | **`date=run_partition`** |
| Silver watermark raw | Có (OHLCV) | **Không** |
| Warehouse upsert key | Theo dataset | **`doc_id`** |
| Gold mart chính | `mart_stock_daily`, … | **`mart_bctc_documents`** |

---

## 12. Tham số `SemiStructuredIngestionConfig` (tham khảo)

| Tham số | Mặc định | Ghi chú |
|---|---|---|
| `data_lake_root` | `data-lake/raw` | |
| `sources` | `["hnx"]` | |
| `tickers` | `[]` | Rỗng = không lọc; 1 mã = filter POST HNX |
| `incremental_window_days` | `30` | Chưa dùng trong crawl |
| `rate_limit_rpm` | `10` (notebook: 20) | |
| `api_retry_max_attempts` | `4` | Crawl POST |
| `download_retry_max_attempts` | `3` | PDF |
| `min_pdf_bytes` | `20000` | |
| `bootstrap_full_history_if_missing` | `True` | Tạo marker lần đầu |
| `full_bootstrap_once_then_incremental` | `False` | |
| `hnx_verify_ssl` | `False` | Dev; production nên `True` |
| `strict_bctc_annual_keyword_filter` | `False` | `True` = siết BCTC năm |
| `ingest_only_financial_statement_vi` | `True` | Override: `BCTC_INGEST_ALL_CRAWLED_PDFS` |
| `ingest_unknown_language_financial` | `True` | |
| `allow_en_docs_for_parse` | `False` | Override: `BCTC_ALLOW_EN_DOCS` |
| `run_partition` | `None` → hôm nay | Partition Bronze/Silver |

---

## 13. Checklist vận hành end-to-end

**Bronze**

1. Mạng tới `www.hnx.vn` / `owa.hnx.vn`; SSL dev nếu cần (`hnx_verify_ssl=False`).
2. `run_bctc_annual_pipeline(cfg, include_download=True)` — ghi nhớ `run_date` trong output.
3. Kiểm tra `bctc_annual_pdf_meta/.../date=<run_date>/PART-000.parquet` và PDF dưới `bctc_annual_pdf/`.

**Silver**

4. `python -m pipeline.silver.cli --dataset bctc_pdf_meta --run-partition <run_date>`.
5. Kiểm tra `silver/bctc_pdf_meta/date=<run_date>/PART-000.parquet` và `_runs.jsonl`.

**Warehouse & Gold**

6. `python -m warehouse.loader.cli load-silver --dataset bctc_pdf_meta` — kiểm tra `silver.load_audit`.
7. `dbt run --profiles-dir transform/dbt --select stg_bctc_pdf_meta+mart_bctc_documents`.
8. `dbt test --profiles-dir transform/dbt --select stg_bctc_pdf_meta mart_bctc_documents`.
9. `SELECT count(*) FROM gold.mart_bctc_documents;` — kỳ vọng subset web-ready (~952 với demo).

**API / UI**

10. `uvicorn backend.main:app --reload --port 8000` + `npm run dev` (frontend).
11. Smoke: `curl http://localhost:8000/bctc/AAV`; mở tab BCTC trên `/stock/AAV`.

`--run-partition` Silver **phải khớp** `date=` partition Bronze của lần ingest tương ứng. Loader đọc **tất cả** partition Silver đã có; upsert `doc_id` giữ bản mới nhất theo `run_partition` / `silver_loaded_at`. Sau load Silver mới, cần **load PG + `dbt run`** lại để `mart_bctc_documents` phản ánh dữ liệu mới.
