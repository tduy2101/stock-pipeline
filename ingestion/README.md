# Ingestion — Tài liệu chi tiết

Thư mục `ingestion/` chứa toàn bộ pipeline **thu thập dữ liệu raw** từ nguồn chứng khoán Việt Nam (vnstock, RSS, HTML tĩnh, link PDF trong tin VCI), ghi ra **data lake** dưới dạng Parquet/PDF.

**Giả định:** luôn chạy Python/Jupyter với **working directory = thư mục gốc repo** (`stock-pipeline/`), hoặc notebook đã thêm `repo_root` vào `sys.path` như các notebook mẫu.

---

## 1. Cấu trúc thư mục code (`ingestion/`)

```
ingestion/
├── common/                    # Re-export helper từ structure_data.common
│   └── __init__.py            # configure_logging, load_dotenv, save_*_parquet, retry, …
├── structure_data/            # Dữ liệu có cấu trúc (vnstock)
│   ├── config.py              # IngestionConfig
│   ├── common.py              # logging, retry, rate limit, save_partition/master, OHLCV
│   ├── pipeline.py            # run_structure_ingestion_pipeline()
│   ├── price_ingestor.py      # Giá cổ phiếu (Quote.history)
│   ├── index_ingestor.py      # Chỉ số (Quote.history)
│   └── stock_info_ingestor.py # listing, company overview, financial ratio, price board
├── unstructured_data/         # Tin tức
│   ├── config.py              # NewsIngestionConfig
│   ├── news_ingestor.py       # ingest_news() — điều phối RSS + HTML
│   ├── rss_adapter.py
│   ├── html_list_adapter.py
│   ├── schema.py              # schema Parquet, normalize/dedupe/validate
│   ├── sources.yaml           # RSS + HTML list (VnExpress, …)
│   └── validate.py
├── semi_structure_data/       # BCTC PDF
│   ├── config.py              # BctcPdfConfig
│   ├── bctc_ingestor.py       # ingest_bctc_pdfs()
│   ├── vnstock_bctc_discovery.py  # Company.news (VCI) + parse PDF trong HTML
│   └── bctc_keywords.py       # Heuristic lọc URL/title “giống BCTC”
├── pipeline_all.py            # **Một file .py** chạy cả 3 luồng + CLI (`python -m ingestion.pipeline_all`)
├── ingest_structure_data_manager.ipynb   # Notebook: chỉ dữ liệu có cấu trúc
├── ingest_news.ipynb          # Notebook: chỉ tin tức
└── ingest_bctc.ipynb          # Notebook: chỉ BCTC PDF
```

---

## 2. Data lake — layout đầu ra (dưới `data-lake/raw/`)

> Thư mục `data-lake/` thường **không commit** (`.gitignore`). Đường dẫn dưới đây là **tương đối** từ `data-lake/raw/`.

### 2.1. Có cấu trúc — `Structure_Data/`

| Thành phần | Đường dẫn | Định dạng | Ghi chú |
|------------|-----------|-----------|---------|
| Giá cổ phiếu | `Structure_Data/price/date=<YYYY-MM-DD>/<TICKER>.parquet` | Parquet | Mỗi mã một file; `run_date` = ngày chạy notebook |
| Chỉ số | `Structure_Data/index/date=<YYYY-MM-DD>/<INDEX_CODE>.parquet` | Parquet | VNINDEX, VN30, HNXINDEX, … |
| Listing (master) | `Structure_Data/listing/master/listing.parquet` | Parquet | **Ghi đè** mỗi lần ingest listing |
| Company overview (master) | `Structure_Data/company/master/company_overview.parquet` | Parquet | **Append** theo batch (`ingest_run_id`) |
| Financial ratio | `Structure_Data/financial_ratio/date=<YYYY-MM-DD>/<TICKER>.parquet` | Parquet | |
| Price board snapshot | `Structure_Data/price_board/date=<YYYY-MM-DD>/PRICE_BOARD_SNAPSHOT.parquet` | Parquet | |

**Gốc logic:** `IngestionConfig.data_lake_root` → `…/data-lake/raw/Structure_Data` (tính từ vị trí `structure_data/config.py`).

### 2.2. Phi cấu trúc — `Unstructure_Data/news/`

| Thành phần | Đường dẫn | Ghi chú |
|-------------|-----------|---------|
| Tin RSS | `Unstructure_Data/news/rss/date=<YYYY-MM-DD>/PART-000.parquet` | |
| Tin HTML list | `Unstructure_Data/news/html/date=<YYYY-MM-DD>/PART-000.parquet` | Khi `enable_html=True` và có spec `enabled` trong `sources.yaml` |

**Cột chuẩn (Parquet):** xem `schema.NEWS_COLUMNS` — `article_id`, `source`, `ticker`, `title`, `summary`, `body_text`, `url`, `published_at`, `fetched_at`, `language`, `raw_ref`.

**Lưu ý ghi file:** luồng news hiện tại chỉ ghi **Parquet** (`PART-000.parquet`), không ghi CSV. Khi `truncate_partition=True`, ingestor dọn `PART-*.parquet` và cả CSV legacy `PART-*.csv` trong partition trước khi ghi mới.

**Gốc logic:** `NewsIngestionConfig.data_lake_root` → `…/data-lake/raw/Unstructure_Data`.

### 2.3. Bán cấu trúc — `Semi_Structure_Data/bctc/`

| Thành phần | Đường dẫn | Ghi chú |
|-------------|-----------|---------|
| PDF đã tải | `Semi_Structure_Data/bctc/pdf/exchange=<EX>/symbol=<SYM>/year=<Y>/q=<Q>/<tên-file>.pdf` | Tên file suy ra từ URL |
| Manifest theo ngày | `Semi_Structure_Data/bctc/manifest/date=<YYYY-MM-DD>/PART-000.parquet` | Mỗi lần chạy có thể append partition |
| Master manifest | `Semi_Structure_Data/bctc/master/bctc_files.parquet` | Gộp theo `file_sha256` (dedup) |

**Cột manifest (tóm tắt):** `symbol`, `exchange`, `year`, `quarter`, `title`, `source_url`, `file_path`, `file_sha256`, `downloaded_at`, `http_status`, `error`, `discovery_source`, `run_date`.

**Gốc logic:** `BctcPdfConfig.data_lake_root` → `…/data-lake/raw/Semi_Structure_Data`.

---

## 3. Đầu vào (input) chung

| Input | Mô tả |
|-------|--------|
| **`.env` (root repo)** | Khuyến nghị: `VNSTOCK_API_KEY=…` — dùng cho vnstock (giá, BCTC discovery). Luồng news RSS/HTML vẫn chạy khi không có key này. |
| **`requirements.txt`** | vnstock, pandas, pyarrow, requests, beautifulsoup4, feedparser, PyYAML, python-dotenv, … |
| **`listing.parquet`** | Bắt buộc cho news (`use_listing_tickers=True`) và BCTC (lọc mã theo sàn). Sinh ra từ `ingest_listing()` trong pipeline có cấu trúc. |
| **`unstructured_data/sources.yaml`** | URL RSS + cấu hình HTML list (selector CSS). |

---

## 4. Luồng 1 — Dữ liệu có cấu trúc (`structure_data`)

### 4.1. API / thư viện

- **vnstock:** `Quote`, `Listing`, `Company`, `Finance`, `Trading` theo `primary_source` / `fallback_source` (mặc định thường `kbs` + `vci`).

### 4.2. Cấu hình — `IngestionConfig` (`structure_data/config.py`)

| Tham số (đại diện) | Ý nghĩa |
|--------------------|---------|
| `tickers` | Danh sách mã cổ phiếu |
| `index_tickers` | Danh sách mã chỉ số |
| `primary_source`, `fallback_source` | Nguồn vnstock (kbs/vci) |
| `rate_limit_rpm`, `years_back`, `max_tickers_per_run` | Giới hạn tốc độ và phạm vi thời gian |
| `delay_between_categories_sec` | Nghỉ giữa các nhóm trong pipeline |

### 4.3. Hàm chính

- **`run_structure_ingestion_pipeline(cfg)`** (`pipeline.py`): tuần tự (có thể tắt từng nhánh qua kwargs)  
  `ingest_prices` → `ingest_indices` → `ingest_listing` → `ingest_company_overview` → `ingest_financial_ratio` → `ingest_price_board`.

### 4.4. Notebook

- **`ingest_structure_data_manager.ipynb`**: setup `sys.path`, UTF-8 guard, gọi từng bước hoặc full pipeline.

---

## 5. Luồng 2 — Tin tức (`unstructured_data`)

### 5.1. Nguồn

1. **RSS** — `requests` + `feedparser`, URL từ `NewsIngestionConfig.rss_feed_urls` + `sources.yaml`.
2. **HTML** — `requests` + BeautifulSoup, `list_url` + `link_css` trong `sources.yaml`.

### 5.2. Cấu hình — `NewsIngestionConfig` (`unstructured_data/config.py`)

| Tham số | Ý nghĩa |
|---------|---------|
| `tickers` | Danh sách mã đầu vào thủ công (fallback khi không đọc listing) |
| `use_listing_tickers` | `True`: đọc mã từ `listing.parquet` (tối đa `max_tickers_per_run`) |
| `listing_exchange_filter` | Lọc sàn (vd. `HSX`, `HNX` — trong listing vnstock sàn chính thường là **HSX**, không phải HOSE) |
| `days_back` | Mốc lọc mặc định theo `published_at`; `0` = không lọc |
| `days_back_rss`, `days_back_html` | Override theo từng nguồn; nếu `None` sẽ fallback về `days_back` |
| `enable_rss`, `enable_html` | Bật từng nguồn |
| `rss_max_per_feed`, `html_max_per_source` | Giới hạn số bài lấy theo feed/source |
| `strict_published_at_days_back` | `True`: lọc strict theo `published_at` hợp lệ + trong khoảng thời gian |
| `enable_ticker_match` | Bật/tắt heuristic map ticker từ `title/summary/body` |
| `append_only`, `truncate_partition` | Luồng hiện dùng fixed file `PART-000.parquet`; khi `truncate_partition=True` sẽ dọn partition trước khi ghi |

### 5.3. Hàm chính

- **`ingest_news(cfg) -> dict[str, dict[str, str] | dict[str, int]]`**
  - Trả về `row_counts` và đường dẫn output theo nguồn đang bật.
  - Cấu trúc điển hình:
    - `row_counts`: `{"rss": <int>, "html": <int>}`
    - `rss`: `{"parquet": ".../news/rss/date=<run_date>/PART-000.parquet"}`
    - `html`: `{"parquet": ".../news/html/date=<run_date>/PART-000.parquet"}`
  - Nếu tắt nguồn nào thì key nguồn đó không xuất hiện.

### 5.4. Notebook

- **`ingest_news.ipynb`**: chỉ tin — cấu hình → `ingest_news` → kiểm tra output parquet theo `rss/html` + validation schema.

---

## 6. Luồng 3 — BCTC PDF (`semi_structure_data`)

### 6.1. Cơ chế

- Đọc **`listing.parquet`**, lọc theo `BctcPdfConfig.exchanges` (mặc định `HSX`, `HNX`, `UPCOM`; chuỗi **`HOSE` được map thành `HSX`** trong ingestor).
- Với mỗi mã: gọi **`Company.news` (VCI)** nhiều trang (`discovery_max_news_pages`), parse HTML cột `news_full_content` / `news_short_content` để tìm thẻ `<a href="...pdf">`.
- **`public_date`** VCI là epoch **milliseconds** — đã parse đúng để lọc theo `quarters_back`.
- Lọc URL/title bằng **`is_bctc_candidate`** (`bctc_keywords.py`) — từ khóa + slug URL kiểu `tai-chinh`, `kiem-toan`, `cbtt`+`thuong-nien`, …
- Tải PDF bằng `requests`, kiểm tra magic `%PDF`, dedup theo SHA-256 trong master manifest.

### 6.2. Cấu hình — `BctcPdfConfig` (`semi_structure_data/config.py`)

| Tham số | Ý nghĩa |
|---------|---------|
| `quarters_back` | Chỉ xét tin có ngày trong cửa sổ quý (tính theo tháng) |
| `exchanges` | Lọc mã theo cột `exchange` trong listing |
| `max_symbols_per_run` | `None` = **tất cả** mã sau lọc (có thể rất lâu) |
| `discovery_max_news_pages` | Số trang tin VCI tối đa / mã |
| `listing_parquet_path` | `None` = đường dẫn mặc định tới `Structure_Data/listing/master/listing.parquet` |

### 6.3. Hàm chính

- **`ingest_bctc_pdfs(cfg, *, refresh_listing=False, structure_cfg=None, vnstock_api_key_env=...)`**  
  Nếu `refresh_listing=True`: gọi lại `ingest_listing(structure_cfg)` trước khi đọc listing.

### 6.4. Notebook

- **`ingest_bctc.ipynb`**: kiểm tra listing → cấu hình → `ingest_bctc_pdfs` → đọc manifest + liệt kê PDF trên đĩa.

---

## 7. Orchestrator — `pipeline_all.py` (một file .py cho cả 3 luồng)

### 7.1. Python API

```python
from ingestion.pipeline_all import run_full_raw_pipeline
from ingestion.structure_data.config import IngestionConfig
from ingestion.unstructured_data import NewsIngestionConfig
from ingestion.semi_structure_data import BctcPdfConfig

out = run_full_raw_pipeline(
    IngestionConfig(),
    NewsIngestionConfig(),
    BctcPdfConfig(),
    include_structure=True,
    include_news=True,
    include_bctc=True,
    bctc_refresh_listing=True,
    delay_between_groups_sec=30,
)
# out["structure"], out["news_paths"], out["news_path"], out["bctc"]
```

### 7.2. Dòng lệnh (từ **root repo**)

```bash
python -m ingestion.pipeline_all
python -m ingestion.pipeline_all --no-structure
python -m ingestion.pipeline_all --no-news --max-tickers 100
python -m ingestion.pipeline_all --no-bctc --max-symbols-bctc 50
```

Tham số CLI: `--no-structure`, `--no-news`, `--no-bctc`, `--delay`, `--max-tickers`, `--max-symbols-bctc`.

---

## 8. Thứ tự chạy khuyến nghị (đồ án / máy mới)

1. Tạo venv, `pip install -r requirements.txt`, copy `.env.example` → `.env` và điền API key.
2. Chạy **`ingest_structure_data_manager.ipynb`** (ít nhất **listing** + các nhóm cần thiết) để có `listing.parquet`.
3. Chạy **`ingest_news.ipynb`** hoặc **`ingest_bctc.ipynb`** độc lập theo nhu cầu.
4. Hoặc một lệnh: **`python -m ingestion.pipeline_all`** (có pause giữa các nhóm).

---

## 9. Giới hạn đã biết (không phải bug “thiếu code”)

- **Tin HTML CafeF / nhiều SPA:** template trong `sources.yaml` thường `enabled: false` — cần API riêng hoặc phân tích network nếu muốn đầy đủ.
- **BCTC:** không crawl trực tiếp HOSE/HNX như trình duyệt; phụ thuộc **tin VCI + link PDF** và heuristic từ khóa.
- **Full listing BCTC:** `max_symbols_per_run=None` × nhiều trang tin = thời gian và rate limit rất lớn; nên chia batch hoặc tăng `rate_limit_rpm` trong giới hạn tài khoản vnstock.

---

*Tài liệu này mô tả trạng thái codebase tại thời điểm viết; khi đổi `config` hoặc layout, cập nhật song song README này.*
