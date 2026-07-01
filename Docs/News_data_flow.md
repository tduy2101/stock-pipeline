# Luồng dữ liệu tin tức (News)

Cập nhật: 2026-06-11

Tài liệu mô tả luồng **tin tức phi cấu trúc** từ crawl đến PostgreSQL:

**Bronze** → **Silver** → **`silver.news`** → **dbt Gold** (`fact_news_article` + aggregate) → **FastAPI** / **React**.

**Tài liệu liên quan:** [Structure](Structure_data_flow.md) · [BCTC](BCTC_data_flow.md) · [README](../README.md)

| Giai đoạn | Code chính | Output |
|---|---|---|
| Bronze | `ingestion/unstructured_data/`, `ingest_news.ipynb` | `data-lake/raw/Unstructured_Data/news/` |
| Silver | `pipeline/silver/news_transformer.py`, `news_validate.py` | `data-lake/silver/news/date=*/` |
| Warehouse | `warehouse/loader/silver_loader.py`, `warehouse/ddl/schema.sql` | `silver.news`, `silver.load_audit` |
| Gold (dbt) | `stg_news`, `fact_news_article`, `mart_stock_news_signal` (+ `int_news_sentiment_daily`, `mart_stock_news_daily` deprecated) | Schema `gold` |
| Note | `mart_stock_news_signal` là mart chính cho stock-detail; `mart_stock_news_daily` chỉ còn backward-compat | Current API/UI |
| API / UI | `news.py`, `NewsPanel.tsx`, `MarketNewsFeed.tsx` | **Từng bài** + sentiment ngày + tin thị trường |

---

## 1. Bối cảnh dự án (tóm tắt)

Dự án **stock-pipeline** xây dựng pipeline Medallion và ứng dụng tra cứu thị trường chứng khoán Việt Nam:

```text
Bronze (raw parquet) → Silver (parquet sạch) → PostgreSQL schema silver → dbt Gold → FastAPI → React
```

Luồng news nằm nhóm **unstructured data** (cùng README: một trong 3 luồng Bronze — structured, news, BCTC PDF). Bronze **không** load thẳng PostgreSQL.

| Thành phần | Vai trò |
|---|---|
| `ingestion/unstructured_data/` | Module Python: fetch RSS/HTML, chuẩn hóa schema, ghi Bronze |
| `ingestion/unstructured_data/sources.yaml` | Danh sách feed RSS và nguồn HTML (selector CSS) |
| `ingestion/ingest_news.ipynb` | Notebook điều phối: cấu hình, chạy `ingest_news`, kiểm tra chất lượng |
| `data-lake/raw/Unstructured_Data/news/` | Bronze root cho tin (gitignore) |
| `data-lake/raw/Structure_Data/listing/master/listing.parquet` | Universe mã CK (Bronze match + Silver `ticker_mentions`) |
| `pipeline/silver/news_transformer.py` | Bronze → Silver: merge rss/html, dedupe `article_id`, enrich |
| `warehouse/loader/silver_loader.py` | Đọc mọi Silver parquet news → upsert PostgreSQL |

---

## 2. Cấu trúc code `ingestion/` (phần news)

```text
ingestion/
├── ingest_news.ipynb              # Điều phối chính (Jupyter)
├── unstructured_data/
│   ├── __init__.py                # Export NewsIngestionConfig, ingest_news, schema helpers
│   ├── config.py                  # NewsIngestionConfig (paths, days_back, rate limit, …)
│   ├── sources.yaml               # RSS feeds + HTML list/detail selectors
│   ├── news_ingestor.py           # ingest_news(), save_news(), orchestration
│   ├── rss_adapter.py             # fetch_rss_news() — feedparser + requests
│   ├── html_list_adapter.py     # fetch_html_list_news() — BeautifulSoup list + detail
│   ├── schema.py                  # NEWS_COLUMNS, article_id, dedupe, validate
│   └── validate.py                # Re-export validate_news_df
└── common/
    └── __init__.py                # configure_logging, rate limit, retry, load .env
```

**Entry point công khai:**

| Hàm / symbol | Mô tả |
|---|---|
| `ingest_news(cfg)` | Pipeline một tầng: RSS và/hoặc HTML → lọc `days_back` → dedupe → validate → ghi parquet theo `run_date` |
| `NewsIngestionConfig` | Dataclass cấu hình toàn bộ hành vi crawl và ghi file |
| `NEWS_COLUMNS` | Schema cố định 11 cột output |

Không có watermark file riêng (khác luồng structured OHLCV). Cửa sổ thời gian dựa trên **`days_back`** + partition theo **ngày chạy ingestion** (`run_date`).

---

## 3. Nguồn dữ liệu và công cụ

### 3.1. Nguồn dữ liệu

Nguồn được khai báo trong `ingestion/unstructured_data/sources.yaml` (có thể bổ sung URL qua `NewsIngestionConfig.rss_feed_urls`). Hai kênh song song:

| Kênh | Cách lấy | Trang / feed (ví dụ) |
|---|---|---|
| **RSS** | HTTP GET feed XML → `feedparser` parse entries | **VnExpress** (kinh doanh, chứng khoán), **CafeF** (home + nhiều chuyên mục), **Vietstock** (cổ phiếu, niêm yết, cổ tức, …) |
| **HTML** | GET trang danh sách → CSS chọn link → (tùy nguồn) GET trang chi tiết → parse title/summary/body/ngày | **VnExpress** (`kinh-doanh`, `chung-khoan`), **CafeF** (`thi-truong-chung-khoan`); Vietstock HTML **tắt** (`enabled: false`) — dùng RSS Vietstock |

Ghi chú vận hành (trong `sources.yaml`):

- CafeF `index.rss` là trang directory — dùng `home.rss` cho entry thật.
- CafeF HTML đôi khi trả khối JS; RSS CafeF là nguồn dự phòng ổn định.
- Vietstock list HTML hay bị chặn/404 khi crawl server-side → ưu tiên RSS.

**Không** dùng vnstock cho news. API key `VNSTOCK_API_KEY` không bắt buộc cho luồng này.

### 3.2. Công cụ và thư viện

| Thành phần | Vai trò |
|---|---|
| **requests** | HTTP session (User-Agent, Accept-Language, header tùy nguồn HTML) |
| **feedparser** | Parse RSS/Atom sau khi tải nội dung feed |
| **BeautifulSoup** (`html.parser`) | Parse trang list/detail HTML; strip HTML trong summary/body |
| **PyYAML** | Đọc `sources.yaml` (`rss_feeds`, `html_sources`) |
| **pandas** | DataFrame giữa các bước; ghi `PART-000.parquet` |
| **ingestion.common** | `wait_for_rate_limit`, `call_with_retry` (exponential backoff), `configure_logging`, `load_dotenv_from_project_root` |
| **pipeline.silver.ticker_match** | `load_stock_universe`, `infer_ticker_from_texts` — gán `ticker` từ title/summary/body (dùng chung logic với Silver) |

Biến môi trường liên quan (notebook):

| Biến | Mặc định / ghi chú |
|---|---|
| `NEWS_RATE_LIMIT_RPM` | Notebook đọc từ `.env`, mặc định thực tế **60** req/phút (config class mặc định **20**) |

Tuân thủ ToS / `robots.txt` của từng site khi chỉnh `sources.yaml` (đã ghi chú trong file).

---

## 4. Điều phối chạy (notebook & CLI)

### 4.1. Notebook `ingest_news.ipynb`

Luồng các cell:

1. **Setup:** UTF-8 console, thêm repo root vào `sys.path`, `configure_logging()`, `load_dotenv_from_project_root()`.
2. **Cấu hình `NewsIngestionConfig`** (profile thực tế trong notebook — backfill-friendly):

   | Tham số | Giá trị notebook | Ý nghĩa |
   |---|---|---|
   | `use_listing_tickers` | `True` | Universe từ `listing.parquet` (HSX/HNX/UPCOM) |
   | `listing_exchange_filter` | `["HSX", "HNX", "UPCOM"]` | Lọc sàn |
   | `max_tickers_per_run` | `5000` | Giới hạn số mã cho match |
   | `days_back` | `30` | Cửa sổ lọc `published_at` (fallback khi `days_back_rss/html` = None) |
   | `days_back_rss` / `days_back_html` | `None` | Dùng chung `days_back` |
   | `strict_published_at_days_back` | `False` | Bài không có `published_at` vẫn giữ (xem mục 5.2) |
   | `rate_limit_rpm` | từ `NEWS_RATE_LIMIT_RPM` (60) | Giãn cách request |
   | `rss_max_per_feed` / `html_max_per_source` | `200` | Cap entry/link mỗi feed/nguồn |
   | `enable_rss` / `enable_html` | `True` | Bật cả hai kênh |
   | `truncate_partition` | `True` | Rerun cùng ngày: xóa `PART-*.parquet` cũ trong partition |
   | `append_only` | `False` | Cờ legacy; layout cố định `PART-000` (append bị bỏ qua trong `save_news`) |

3. **Chạy:** `news_paths = ingest_news(news_cfg)`.
4. **Kiểm tra:** đọc parquet, `validate_news_df`, preview, báo cáo chất lượng theo mọi `date=*` dưới `news_root`.

`run_date` = **`date.today().isoformat()`** (property `NewsIngestionConfig.run_date`), không phải ngày đăng bài.

### 4.2. Chạy từ Python (không notebook)

```powershell
@'
from ingestion.unstructured_data import NewsIngestionConfig, ingest_news

cfg = NewsIngestionConfig(days_back=30)
print(ingest_news(cfg))
'@ | python -
```

README gợi ý `days_back=30` cho lần backfill; production hàng ngày thường `days_back=1` (mặc định class).

### 4.3. Notebook vs DAG Airflow

| | Notebook `ingest_news.ipynb` | DAG `news_daily` / `NewsIngestionConfig()` |
|---|---|---|
| Schedule | Thủ công | Daily **06:00 ICT** |
| File DAG | — | `docker/airflow/dags/news_daily.py` |
| `days_back` | `30` | `1` |
| Partition Bronze | `date=<run_date>` (ngày chạy job) | Cùng — một partition mỗi lần ingest |
| Silver | `--run-partition <run_date>` khớp Bronze | XCom từ `bronze_news` (ngày ingest thực tế) |
| dbt subset | — | `+mart_stock_news_signal +fact_news_article` |
| Listing cho match ticker | `use_listing_tickers=True` (nên chạy sau `ingest_listing`) | Mặc định class `False` |

Runbook: [docker/airflow/README_airflow.md](../docker/airflow/README_airflow.md)

Backfill 30 ngày **không** tạo 30 partition Bronze; mọi bài trong cửa sổ nằm chung partition ngày chạy. Muốn Silver theo từng ngày đăng cần nhiều lần ingest hoặc logic riêng (chưa có).

### 4.4. Tóm tắt backfill -> DAG cho news

| Bước | Backfill notebook | DAG `news_daily` |
|---|---|---|
| Phạm vi crawl | `days_back=30`, bật cả RSS + HTML | `days_back=1`, bật cả RSS + HTML |
| Universe ticker match | Thường bật `use_listing_tickers=True` để match theo listing | Mặc định class `False`, tức vẫn crawl tin nhưng không buộc phải sync listing trước |
| Bronze partition | `data-lake/raw/Unstructured_Data/news/<rss|html>/date=<run_date>/PART-000.parquet` | Cùng layout, mỗi ngày một `run_partition` mới |
| Silver | `data-lake/silver/news/date=<run_date>/PART-000.parquet` | Cùng layout; task silver đọc `run_partition` do bronze trả về qua XCom |
| Warehouse / Gold | `silver.news` -> `gold.fact_news_article` + `gold.mart_stock_news_signal` | Cùng luồng; sau load + dbt thì API/UI đọc dữ liệu mới |

Điểm quan trọng:

- Partition Bronze/Silver của `news` là **ngày chạy job**, không phải `published_at`.
- “Incremental” của DAG news là thu hẹp cửa sổ crawl còn `1` ngày; lịch sử nhiều ngày được tích lũy ở Silver/PostgreSQL qua nhiều `run_partition`.
- Khi rerun cùng ngày, notebook thường dùng `truncate_partition=True`, nên partition Bronze ngày đó được ghi lại từ đầu.

### 4.5. HTML sources (`sources.yaml`, 2026-06)

| Nguồn | `enabled` | Ghi chú |
|---|---|---|
| VnExpress (kinh doanh, chứng khoán) | yes | CSS + `detail_mode: hybrid` |
| CafeF thị trường CK | yes | RSS là fallback chính nếu HTML JS-only |
| **VnEconomy** (thị trường, kinh tế số, tài chính) | yes | `link_css` theo layout Hemera CMS; hybrid + OpenGraph |
| Vietstock HTML | no | Ưu tiên RSS (hay bị chặn) |

**Hybrid HTML:** YAML CSS trước → thiếu field thì `article_heuristics` (JSON-LD, Open Graph, DOM). Gợi ý selector:

```powershell
python -m ingestion.unstructured_data.html_discovery --url "https://vneconomy.vn/thi-truong.htm"
```

Module: `ingestion/unstructured_data/article_heuristics.py`, `html_discovery.py`, `html_list_adapter.py`.

---

## 5. Cơ chế lấy dữ liệu

### 5.1. Kiến trúc “one-layer” Bronze

`ingest_news` gom fetch + chuẩn hóa + ghi disk trong một lớp (không tách raw JSON trung gian):

```mermaid
flowchart LR
  subgraph sources [Nguồn]
    YAML[sources.yaml]
    RSSfeeds[RSS URLs]
    HTMLpages[HTML list URLs]
  end
  subgraph ingest [ingest_news]
    A[fetch_rss_news]
    B[fetch_html_list_news]
    F["_filter_days_back"]
    D[dedupe_news per category]
    V[validate_news_df]
    S[save_news]
  end
  YAML --> A
  YAML --> B
  RSSfeeds --> A
  HTMLpages --> B
  A --> F
  B --> F
  F --> D --> V --> S
  S --> P1["news/rss/date=.../PART-000.parquet"]
  S --> P2["news/html/date=.../PART-000.parquet"]
```

### 5.2. RSS (`rss_adapter.py`)

Với mỗi feed trong merged spec (`cfg.rss_feed_urls` + `sources.yaml`):

1. `wait_for_rate_limit(rate_limit_rpm)`.
2. `requests.Session.get(feed_url)` + `call_with_retry` (tối đa `api_retry_max_attempts`, backoff `api_retry_base_delay_sec`).
3. `feedparser.parse(content)` — tối đa `rss_max_per_feed` (hoặc `max_articles_per_source`) entry đầu feed.
4. Mỗi entry: `title`, `url` (bắt buộc), `summary`/`content` (strip HTML), `published_at` (nhiều field fallback → ISO UTC `Z`).
5. `source` = `{label}_rss` hoặc `rss_{hostname}`.
6. `ticker` = `infer_ticker_with_universe([title, summary, body], universe)` nếu `enable_ticker_match`.
7. `article_id` = SHA-256(`normalize_url(url)`) hoặc hash composite khi không có URL.
8. `fetched_at` = thời điểm batch (UTC), `language` = `"vi"`, `raw_ref` = JSON entry gốc.
9. Dedupe trong adapter: `dedupe_news` theo `(source, article_id)`.

Feed lỗi: log warning, **bỏ qua feed**, tiếp tục feed khác.

### 5.3. HTML (`html_list_adapter.py`)

Với mỗi spec `html_sources` (`enabled: true`):

1. GET `list_url`, parse `link_css` → danh sách anchor (tối đa `html_max_per_source`).
2. Nếu có block `detail`:
   - GET từng URL bài (rate limit giữa request).
   - Parse `title_css`, `summary_css`, `body_css`.
   - `published_at`: parser riêng **vnexpress_html** / **cafef_html** (timezone `Asia/Ho_Chi_Minh` → UTC).
   - Nếu có `detail` mà **không** parse được `published_at` **và** `body_text` rỗng → **bỏ** bài (tránh list-only rác).
3. `source` = `{source_label}_html`.
4. Cùng logic `article_id`, `ticker`, `fetched_at`, `raw_ref` (metadata crawl, không full HTML gốc).

Nguồn HTML Vietstock trong yaml hiện **`enabled: false`**.

### 5.4. Lọc thời gian (`days_back`)

Sau fetch, **theo từng category** (`rss` / `html`):

- `days_back` = `days_back_rss` hoặc `days_back_html` nếu set, ngược lại `cfg.days_back`.
- **`strict_published_at_days_back=False`** (mặc định): giữ bài có `published_at` null; chỉ loại bài có ngày **cũ hơn** cutoff `now - days_back`.
- **`strict=True`**: chỉ giữ bài có `published_at` hợp lệ trong cửa sổ; bài thiếu ngày bị loại.

Dedupe lần nữa trước validate: `dedupe_news` (trong cùng `source`).

### 5.5. Backfill vs lần chạy tiếp theo (incremental theo nghĩa vận hành)

**Không** có file watermark JSON như OHLCV structured. Cơ chế như sau:

| Chế độ | Cấu hình điển hình | Hành vi |
|---|---|---|
| **Backfill / lần đầu** | `days_back=30` (notebook), `truncate_partition=True` | Mỗi feed/HTML lấy tới 200 bài mới nhất; sau lọc giữ bài đăng trong ~30 ngày (và bài thiếu ngày nếu non-strict). Ghi vào `date=<run_date_hôm_nay>/`. Ví dụ chạy 2026-05-19: ~820 dòng RSS + 82 HTML trong một partition. |
| **Chạy hàng ngày** | `days_back=1` (default class) | Chỉ giữ tin `published_at` trong 24h gần nhất (plus null nếu non-strict). Partition mới mỗi ngày calendar (`run_date`). Lịch sử Bronze = nhiều thư mục `date=YYYY-MM-DD/`. |
| **Rerun cùng ngày** | `truncate_partition=True` | Xóa `PART-*.parquet` (và csv nếu có) trong `news/<rss\|html>/date=<run_date>/`, ghi lại `PART-000.parquet` — **không merge** với file cũ trong partition đó. |
| **Rerun khác ngày** | — | Tạo partition ngày mới; partition cũ giữ nguyên (append theo thời gian ingestion, không theo `published_at`). |

**Lưu ý partition:** key partition Bronze = **ngày chạy pipeline** (`run_date`), không phải ngày đăng tin. Silver **không** tự quét mọi `date=*` Bronze — mỗi lần transform chỉ đọc **một** `run_partition` (phải trùng ngày Bronze vừa crawl). Dedupe RSS+HTML và gộp lịch sử nhiều ngày xảy ra ở bước **load DB** (đọc toàn bộ Silver parquet).

**Trùng lặp cross-source:** Bronze cho phép cùng URL/bài xuất hiện ở `rss` và `html` (dedupe chỉ trong từng `source`). Ví dụ quality report một lần chạy: 902 dòng, 804 `article_id` unique — trùng giữa kênh hoặc label feed khác nhau.

### 5.6. Gán mã cổ phiếu (`ticker`)

| Cấu hình | Universe |
|---|---|
| `use_listing_tickers=False` | `cfg.tickers` (list tĩnh, tối đa `max_tickers_per_run`) |
| `use_listing_tickers=True` | Đọc `listing.parquet`, lọc `listing_exchange_filter`; fallback `load_stock_universe` (logic Silver — stock-only, blocklist token như CEO, HCM, …) |

Match trên `title`, `summary`, `body_text` khi crawl (Bronze đã có `ticker` sơ bộ; Silver có thể enrich lại).

---

## 6. Output Bronze

### 6.1. Đường dẫn và layout

```text
data-lake/raw/Unstructured_Data/news/
├── rss/
│   └── date=<YYYY-MM-DD>/          # run_date = ngày chạy ingest
│       └── PART-000.parquet
└── html/
    └── date=<YYYY-MM-DD>/
        └── PART-000.parquet
```

Ví dụ (notebook 2026-05-19):

```text
data-lake/raw/Unstructured_Data/news/rss/date=2026-05-19/PART-000.parquet   # 820 rows
data-lake/raw/Unstructured_Data/news/html/date=2026-05-19/PART-000.parquet  # 82 rows
```

Hàm `ingest_news` trả về:

```python
{
  "rss": {"parquet": "<path>"},
  "html": {"parquet": "<path>"},
  "row_counts": {"rss": 820, "html": 82},
}
```

### 6.2. Schema `NEWS_COLUMNS`

| Cột | Kiểu / ghi chú |
|---|---|
| `article_id` | `str` — SHA-256 URL chuẩn hóa (hoặc hash fallback) |
| `source` | `str` — ví dụ `vnexpress_kinh_doanh_rss`, `cafef_html` |
| `ticker` | `str \| null` — mã CK suy ra từ nội dung |
| `title` | `str` — bắt buộc, không rỗng |
| `summary` | `str` — thường từ RSS/HTML sapo |
| `body_text` | `str` — nội dung plain; RSS thường rỗng nếu feed không có full text |
| `url` | `str` — http(s), UTM query stripped |
| `published_at` | `datetime64[ns, UTC]` khi ghi parquet |
| `fetched_at` | `datetime64[ns, UTC]` — thời điểm batch crawl |
| `language` | `str` — mặc định `"vi"` |
| `raw_ref` | `str` — JSON metadata/entry gốc (debug, không dùng UI) |

### 6.3. Validation trước khi ghi

`validate_news_df` (fail hard → `ValueError`):

- Đủ cột `NEWS_COLUMNS`.
- `article_id`, `title` không rỗng.
- `url` phải bắt đầu `http`.
- Cảnh báo nếu >20% dòng có `body_text == title`.

### 6.4. Chất lượng tham khảo (một lần chạy notebook)

Partition `date=2026-05-19` (backfill 30 ngày):

| Nguồn file | Rows | `published_at` min → max (UTC) |
|---|---|---|
| rss | 820 | 2026-04-19 → 2026-05-19 |
| html | 82 | 2026-05-11 → 2026-05-19 |

Tổng 902 dòng / 804 `article_id` unique — dedupe Silver sẽ xử lý gộp RSS+HTML.

---

## 7. Sơ đồ end-to-end

```text
sources.yaml / RSS / HTML
        │
        ▼  ingest_news (Bronze)
raw/.../news/{rss|html}/date=<run_date>/PART-000.parquet
        │
        ▼  run_news_silver --run-partition <run_date>
silver/news/date=<run_date>/PART-000.parquet  +  _runs.jsonl
        │
        ▼  load-silver --dataset news (mọi date=* Silver)
PostgreSQL silver.news  (PK article_id, UNIQUE url)
        │
        ▼  dbt (ngoài phạm vi chi tiết)
gold.mart_stock_news_signal → GET /news/{symbol}
        │
        └── (join) gold.mart_stock_daily news fields → /prices, /indicators
```

**Grain hiện tại của analytics tin:** `gold.fact_news_article` ở mức `article_id + ticker` để phục vụ archive/article search; `gold.mart_stock_news_signal` ở mức `ticker + trading_date` để phục vụ stock-detail theo phiên giao dịch. `mart_stock_news_daily` (`ticker + published_date`) chỉ còn là aggregate legacy/backward-compatible.

---

## 8. Silver (`pipeline/silver`)

### 8.1. Cấu trúc code

```text
pipeline/silver/
├── config.py                 # SilverConfig, news_bronze_path()
├── news_transformer.py       # transform_news(), run_news_silver()
├── news_validate.py          # validate_news_silver() — ERROR vs WARN
├── ticker_match.py           # load_stock_universe, find_mentions_in_parts, …
├── bronze_reader.py          # write_single_part_parquet()
├── runs_log.py               # Ghi audit _runs.jsonl
└── cli.py                    # --dataset news --run-partition … --strict
```

**Lưu ý:** `python -m pipeline.silver.cli --dataset all` **không** chạy `news` (chỉ structured datasets). News phải gọi riêng với `--run-partition`.

### 8.2. Đầu vào Bronze (theo từng lần transform)

`SilverConfig.news_bronze_path(stream, run_partition)` trỏ tới:

```text
data-lake/raw/Unstructured_Data/news/html/date=<run_partition>/PART-000.parquet
data-lake/raw/Unstructured_Data/news/rss/date=<run_partition>/PART-000.parquet
```

`_read_news_inputs()`:

- Đọc lần lượt stream **`html`** rồi **`rss`** (thiếu file → log skip, không fail).
- Gắn metadata: `source_file`, `run_partition`, `_source_priority` (html=0, rss=1 — html ưu tiên khi trùng `article_id`).
- `concat` thành một DataFrame thô.

**`run_partition` CLI phải khớp `run_date` Bronze** (ví dụ cùng `2026-05-19`). Chạy Silver cho ngày D mà chưa ingest Bronze ngày D → output rỗng (0 dòng) nhưng vẫn có thể ghi parquet/audit tùy validation.

### 8.3. Transform (`transform_news`)

| Bước | Hành vi |
|---|---|
| Lọc `article_id` | Bỏ dòng rỗng/invalid; ghi DQ warning |
| Chuẩn hóa text | `normalize_text` / `normalize_text_series` cho title, summary, body |
| Datetime | `published_at`, `fetched_at` → UTC |
| Dedupe grain | **`article_id`** (cross-stream RSS+HTML). Sort: `_body_len` ↓, `_has_published_at` ↓, `_source_priority` ↑ → **giữ html trước rss** khi cùng URL/hash |
| Ticker | `load_stock_universe(listing.parquet)` → `ticker_mentions` (list mã trong bài); `ticker` = bronze nếu nằm trong mentions, else `mentions[0]` hoặc `pick_primary_ticker` |
| Sentiment | `keyword_v1`: đếm từ POSITIVE_KEYWORDS vs NEGATIVE_KEYWORDS trên title+summary+body → `sentiment_score`, `sentiment_label` (`positive`/`negative`/`neutral`) |
| Derived | `published_date` = date(`published_at`), `word_count`, `silver_loaded_at` = thời điểm transform |
| Output sort | Theo `article_id` |

Ví dụ thực tế (Bronze 2026-05-19: 902 dòng rss+html) → Silver ~**804** dòng (một `article_id` / bài).

### 8.4. Schema Silver output (`NEWS_OUTPUT_COLUMNS`)

| Cột | Mô tả |
|---|---|
| `article_id` | PK logic; giữ từ Bronze |
| `source` | Nguồn feed/html đã chọn sau dedupe |
| `ticker` | Mã chính (có thể null) |
| `ticker_mentions` | `list[str]` — mọi mã match trong bài |
| `title`, `summary`, `body_text`, `url` | Đã normalize |
| `published_at`, `published_date`, `fetched_at` | UTC / date |
| `language` | Thường `vi` |
| `word_count` | Số token (regex `\S+`) |
| `sentiment_score`, `sentiment_label`, `sentiment_method` | Baseline `keyword_v1` |
| `raw_ref` | Metadata Bronze |
| `run_partition` | Ngày Bronze đã đọc |
| `source_file` | Đường dẫn parquet Bronze nguồn |
| `silver_loaded_at` | Timestamp transform |

### 8.5. Validation (`news_validate.py`)

| Mức | Điều kiện |
|---|---|
| **ERROR** (fail nếu `--strict`) | Thiếu cột; `article_id` rỗng/trùng; `url` không http khi có; `title` rỗng; `url` trùng trong batch |
| **WARN** | >30% `ticker` thuộc `TICKER_BLOCKLIST`; `ticker` không khớp universe/`ticker_mentions` |

`run_news_silver`: validate → `write_single_part_parquet` → `write_runs_entry` vào `data-lake/silver/news/_runs.jsonl`.

### 8.6. Output Silver

```text
data-lake/silver/news/date=<run_partition>/PART-000.parquet
data-lake/silver/news/_runs.jsonl
```

Ghi đè `PART-*.parquet` cũ trong cùng thư mục partition (giống Bronze). Nhiều ngày chạy → nhiều thư mục `date=*` — loader DB đọc **tất cả**.

### 8.7. Chạy Silver

```powershell
python -m pipeline.silver.cli --dataset news --run-partition 2026-05-19 --strict
```

| Tham số | Bắt buộc | Ghi chú |
|---|---|---|
| `--run-partition` | Có | ISO date `YYYY-MM-DD`, trùng Bronze `run_date` |
| `--strict` | Không | Raise nếu có message `ERROR:` từ validation |

Grain Silver (README): **`article_id`** — dedupe RSS/HTML; enrich ticker/sentiment baseline.

---

## 9. Load PostgreSQL (`warehouse/loader`)

### 9.1. CLI

```powershell
$env:DATABASE_URL = "postgresql://stock:stock@localhost:55432/stock_pipeline"
python -m warehouse.loader.cli load-silver --dataset news
```

`--dataset all` chạy theo thứ tự `DATASET_ORDER`; `news` đứng sau `price_board`, trước `bctc_pdf_meta`.

### 9.2. Đọc parquet

- **Glob:** `data-lake/silver/news/**/*.parquet` (mọi partition `date=*`, **không** đọc `_runs.jsonl`).
- `pd.concat` toàn bộ file → một DataFrame.

### 9.3. Chuẩn bị & dedupe trước upsert

`prepare_dataframe()` cho dataset `news`:

| Xử lý | Chi tiết |
|---|---|
| Cột | Giữ đúng 20 cột trong `DATASET_CONFIG["news"]` |
| Kiểu | `published_date`, `run_partition` → `date`; timestamps UTC; `ticker_mentions` → PostgreSQL `text[]`; `raw_ref` → `jsonb` |
| **`dedupe_on_load`** | Sort `run_partition`, `silver_loaded_at`, `published_at`, `fetched_at` → `drop_duplicates(article_id, keep="last")` — bản mới nhất thắng khi nhiều partition Silver |
| Key quality | Không null/trùng `article_id` |
| **Unique** | `url` non-null không được trùng trong batch load |

### 9.4. Bảng `silver.news` (DDL)

```sql
-- warehouse/ddl/schema.sql (rút gọn)
PRIMARY KEY (article_id)
UNIQUE INDEX silver_news_url_uq ON silver.news(url) WHERE url IS NOT NULL
loaded_at timestamptz NOT NULL DEFAULT now()  -- thời điểm load DB, khác silver_loaded_at
```

Upsert: `INSERT … ON CONFLICT (article_id) DO UPDATE` — cập nhật mọi cột non-key; idempotent khi chạy lại loader.

### 9.5. Audit

Mỗi lần load ghi `silver.load_audit`: `dataset`, `run_partition` (max trong batch), `rows_read`, `rows_inserted`, `rows_updated`, `status`, `error_msg`.

### 9.6. Khác biệt Bronze vs Silver vs DB

| Khía cạnh | Bronze | Silver (một run) | PostgreSQL |
|---|---|---|---|
| Partition key | `run_date` ingest | `run_partition` (= ngày Bronze đọc) | Không partition — một bảng |
| Dedupe | Trong từng `source` | Cross rss/html theo `article_id` | Cross mọi file Silver theo `article_id` |
| Phạm vi một lần chạy | 1 ngày ingest | 1 ngày Bronze | Toàn bộ lịch sử Silver parquet |
| Sentiment | Không | `keyword_v1` | Lưu trong `silver.news` |

---

## 10. Gold — dbt (`transform/dbt`)

### 10.1. Cấu trúc project

```text
transform/dbt/
├── dbt_project.yml          # profile stock_pipeline; staging=view, intermediate/marts=table
├── profiles.yml             # Postgres localhost:55432, schema gold
└── models/
    ├── staging/
    │   ├── sources.yml      # source('silver', 'news')
    │   ├── schema.yml       # tests stg_news
    │   └── stg_news.sql     # materialized='ephemeral'
    ├── intermediate/
    │   └── int_news_sentiment_daily.sql
    └── marts/
        ├── mart_stock_news_daily.sql
        ├── mart_stock_daily.sql      # LEFT JOIN mart_stock_news_signal (luồng giá + news signal)
        └── schema.yml                # tests grain ticker+published_date
```

Chạy từ **repo root** (launcher `dbt_project.yml` ở root trỏ `transform/dbt/models`):

```powershell
dbt debug --profiles-dir transform/dbt
dbt run --profiles-dir transform/dbt
dbt test --profiles-dir transform/dbt
```

- **Target schema:** `gold` (`profiles.yml` → `schema: gold`).
- **Điều kiện:** PostgreSQL đã có `silver.news` (sau `load-silver --dataset news`). dbt **không** đọc parquet Silver trên disk.
- **`dbt run --dataset all` (pipeline silver CLI)** khác **`dbt run`**: lệnh dbt build toàn bộ graph (price + news + …) miễn là silver PG đủ bảng.

### 10.2. Source

`models/staging/sources.yml`:

```yaml
sources:
  - name: silver
    schema: silver
    tables:
      - name: news   # article_id, ticker, published_date, sentiment_*, title, url, …
```

### 10.3. Staging — `stg_news` (ephemeral)

File: `models/staging/stg_news.sql`

- Giữ mọi bài có `published_date`, `article_id`, `title`.
- **Explode ticker:** gộp `ticker` + `ticker_mentions` → `unnest` — mỗi `(article_id, ticker)` là một dòng downstream.
- Suy ra **`ticker_relevance`** (`title` > `summary` > `body`) và **`source_tier`** (Vietstock/CafeF/khác) phục vụ weighted sentiment.
- **`body_text`** vẫn đi vào `fact_news_article` (API trả summary/body).

| Đặc điểm | Chi tiết |
|---|---|
| Materialization | **`ephemeral`** — không tạo bảng `gold.stg_news` |
| Lọc | Chỉ bỏ bài thiếu ngày đăng / `article_id` / `title`; bài không match mã nào không vào explode |
| Grain | **`article_id + ticker`** (một bài nhắc nhiều mã → nhiều dòng) |
| Sentiment | Từ Silver (`keyword_v1`), Gold không tính lại |

**dbt tests:** `article_id` + `ticker` not_null; unique composite `(article_id, ticker)`; `published_date` not_null.

### 10.3b. Mart bài báo — `fact_news_article` (table)

File: `models/marts/fact_news_article.sql` — pass-through `stg_news` (title, summary, `body_text`, url, sentiment, `ticker_relevance`, `source_tier`, …).

| Object | Grain | API |
|---|---|---|
| **`gold.fact_news_article`** | `article_id + ticker` | `GET /news/articles`, `GET /news/{symbol}/articles`, `GET /news/market` |

Đây là nguồn **tin đầy đủ từng bài** (không gom ngày). Aggregate ngày legacy vẫn ở `mart_stock_news_daily` (deprecated); API/UI chính dùng `mart_stock_news_signal`.

### 10.4. Intermediate — `int_news_sentiment_daily` (table)

File: `models/intermediate/int_news_sentiment_daily.sql`

```sql
select
  ticker,
  published_date,
  count(*) as news_count,
  avg(sentiment_score) as avg_sentiment_score,
  count(*) filter (where sentiment_label = 'positive') as positive_count,
  count(*) filter (where sentiment_label = 'negative') as negative_count,
  count(*) filter (where sentiment_label = 'neutral') as neutral_count,
  mode() within group (order by sentiment_label) as dominant_sentiment
from {{ ref('stg_news') }}
where ticker is not null
group by ticker, published_date
```

| Cột | Ý nghĩa |
|---|---|
| `news_count` | Số bài có cùng ticker (đã resolve) + ngày đăng |
| `avg_sentiment_score` | Trung bình điểm keyword_v1 trong ngày |
| `positive_count` / `negative_count` / `neutral_count` | Đếm nhãn từng bài |
| `dominant_sentiment` | `mode()` — nhãn xuất hiện nhiều nhất (hòa có thể lấy theo thứ tự PostgreSQL) |

- **Materialization:** `table` → `gold.int_news_sentiment_daily`.
- **Grain:** **`ticker + published_date`** (unique test trong `marts/schema.yml`).
- Ví dụ demo cũ: ~804 bài Silver → ~**154** cặp (ticker, ngày); workspace 2026-06-03: **924** bài Silver.

### 10.5. Mart — `mart_stock_news_daily` (table)

> WARNING: Deprecated: giữ lại để backward-compat. API/UI chính đã chuyển sang `mart_stock_news_signal`. Sẽ bỏ sau khi UI ổn định.

File: `models/marts/mart_stock_news_daily.sql`

- Pass-through từ `int_news_sentiment_daily`, **`round(avg_sentiment_score::numeric, 4)`**.
- **Materialization:** `table` → **`gold.mart_stock_news_daily`**.
- **Grain:** `ticker + published_date` — legacy/simple aggregate; không còn là nguồn chính cho API/UI stock-detail.

### 10.6. Liên kết luồng giá — `mart_stock_daily`

File: `models/marts/mart_stock_daily.sql`

```sql
select
  fpd.*,
  coalesce(ns.news_count, 0) as news_count,
  ns.avg_sentiment_score,
  ns.weighted_sentiment,
  ns.news_signal,
  ns.dominant_sentiment,
  ns.top_articles
from {{ ref('fact_price_daily') }} fpd
left join {{ ref('mart_stock_news_signal') }} ns
  on fpd.ticker = ns.ticker
  and fpd.trading_date = ns.trading_date
```

| Ý nghĩa | Chi tiết |
|---|---|
| Join key | **`fact_price_daily.trading_date` = `mart_stock_news_signal.trading_date`** (đã map weekend/sau cutoff sang phiên giao dịch) |
| Khi không có tin | `news_count = 0`; `avg_sentiment_score` / `dominant_sentiment` = NULL |
| API hiện tại | `GET /prices/{symbol}` và `GET /indicators/{symbol}` đọc `mart_stock_daily`; output có thể mang các trường news đã nhúng tùy schema/API response |
| DDL legacy | `warehouse/ddl/schema.sql` có `sentiment_score` / `sentiment_label` trên `mart_stock_daily`; model dbt hiện tại thêm `news_count`, `avg_sentiment_score`, `dominant_sentiment` — có thể lệch schema DB cũ nếu chưa `dbt run` sau đổi model |

### 10.7. DAG phụ thuộc (dbt lineage + Airflow)

**Orchestration:** DAG `news_daily` — `bronze_news` → `silver_news` → `load_silver` → `dbt_marts` (selector ở §4.3).

```mermaid
flowchart TB
  sn[silver.news]
  stg[stg_news ephemeral]
  int[int_news_sentiment_daily]
  msn[mart_stock_news_signal]
  fpd[fact_price_daily]
  msd[mart_stock_daily]

  sn --> stg
  stg --> int
  stg --> fna[fact_news_article]
  int --> msnd[mart_stock_news_daily deprecated]
  fna --> msn
  msn --> msd
  fpd --> msd
```

Tin từng bài: **`gold.fact_news_article`**; ad-hoc sâu hơn: `silver.news` (có `raw_ref`).

### 10.8. Output cuối cùng — bảng Gold

| Object | Loại | Grain | Nội dung |
|---|---|---|---|
| `stg_news` | ephemeral | `article_id + ticker` | Explode `ticker_mentions`; thêm `ticker_relevance`, `source_tier` |
| **`fact_news_article`** | table | `article_id + ticker` | **Danh sách bài** cho API articles/archive |
| **`mart_stock_news_signal`** | table | `ticker + trading_date` | Signal theo phiên GD; API `/news/{symbol}`, `/news/{symbol}/signal` |
| `int_news_sentiment_daily` | table | `ticker + published_date` | Aggregate legacy (feed `mart_stock_news_daily`) |
| **`mart_stock_news_daily`** | table | `ticker + published_date` | WARNING: Deprecated; không còn là nguồn API/UI chính |
| `mart_stock_daily` (cột news) | incremental | `ticker + trading_date` | Join `mart_stock_news_signal` vào giá/chỉ báo |

**Snapshot workspace (2026-06-03):**

| Layer | Rows | Ghi chú |
|---|---:|---|
| Bronze RSS `date=2026-06-03` | ~934 | |
| Bronze HTML `date=2026-06-03` | ~111 | VnEconomy + VnExpress + CafeF |
| Silver `news/date=2026-06-03` | 924 | Dedupe RSS+HTML |
| `gold.*` (sau `dbt run`) | (theo PG) | Ví dụ demo cũ: `mart_stock_news_daily` (deprecated) ~154 cặp |

---

## 11. API & Frontend (output ứng dụng)

### 11.1. FastAPI

Router: `backend/routers/news.py` — prefix `/news`, tag `news`.

| Endpoint | Nguồn DB | Hành vi |
|---|---|---|
| **`GET /news/articles`** | `gold.fact_news_article` | Danh mục bài (paginate); query `ticker`, `q` (title/summary/body), `sentiment`, `from`, `to` |
| **`GET /news/{symbol}/articles`** | `gold.fact_news_article` | Bài theo mã: `ticker = symbol` **hoặc** `symbol ∈ ticker_mentions` |
| `GET /news/{symbol}` | `gold.mart_stock_news_signal` | Weighted signal theo phiên giao dịch |
| `GET /news/market` | `gold.fact_news_article` | Top bài thị trường — dashboard `MarketNewsFeed` |

**Response models** (`backend/schemas/news.py`):

| Endpoint | Schema | Ghi chú |
|---|---|---|
| `GET /news/{symbol}`, `GET /news/{symbol}/signal` | `NewsSignalRow` / `NewsSignalSummary` | `trading_date`, `weighted_sentiment`, `news_signal`, `top_articles` |
| `GET /news/articles`, `GET /news/{symbol}/articles` | `NewsArticleRow` | `published_date`, `title`, `summary`, `body_text`, `ticker_relevance`, `source_tier` |

Backend **chỉ đọc schema `gold`**, không đọc `silver.news` hay parquet.

Ví dụ:

```powershell
curl "http://localhost:8000/news/articles?ticker=FPT&page_size=20"
curl "http://localhost:8000/news/FPT/articles?page_size=50"
curl "http://localhost:8000/news/FPT/articles?from=2026-05-01&to=2026-05-19"
curl "http://localhost:8000/news/FPT?page_size=30"
curl "http://localhost:8000/news/market?page_size=15"
```

### 11.2. React / Vite

| Thành phần | Vai trò |
|---|---|
| `fetchNewsArticles` / `fetchAllNewsArticles` | `/news/{symbol}/articles`, `/news/articles` |
| `NewsPanel` | Sentiment theo ngày + **danh sách bài** (link, sapo/body) |
| `MarketNewsFeed` | Dashboard — `GET /news/market` |
| `useMarketNews` | Hook cho feed thị trường |

### 11.3. Phạm vi MVP (giới hạn còn lại)

- Bài **không** nhắc mã CK nào (`ticker` và `ticker_mentions` rỗng) không hiện khi lọc theo symbol; vẫn có thể xem qua `/news/market`.
- Một bài nhắc nhiều mã chỉ đếm sentiment cho **một** `ticker` resolve (chưa explode multi-ticker).
- Không ML sentiment nâng cao (README: out of scope).

---

## 14. Lấy tin tức đầy đủ (hướng dẫn vận hành)

### 14.1. Ba lớp “đầy đủ”

| Mức | Dữ liệu | Cách lấy |
|---|---|---|
| **Bronze/Silver** | Mọi bài crawl (kể cả chưa gán mã) | `silver.news` sau ingest + transform + load |
| **Gold bài báo** | Bài có title + ngày đăng | `gold.fact_news_article` sau `dbt run` |
| **UI/API** | Bài liên quan mã X | `GET /news/X/articles` (+ mentions trong `ticker_mentions`) |

### 14.2. Ingest nhiều tin hơn (Bronze)

```powershell
@'
from ingestion.unstructured_data import NewsIngestionConfig, ingest_news
cfg = NewsIngestionConfig(
    days_back=30,
    use_listing_tickers=True,
    enable_rss=True,
    enable_html=True,
    rss_max_per_feed=200,
    html_max_per_source=200,
)
print(ingest_news(cfg))
'@ | python -
```

- **`days_back=30`:** backfill ~30 ngày `published_at` (daily: `days_back=1`).
- **`use_listing_tickers=True`:** match mã tốt hơn → nhiều bài có `ticker` / `ticker_mentions` hơn.
- Chạy **Silver + load + dbt** cho từng `run_partition` (mỗi ngày ingest).

### 14.3. Build Gold & kiểm tra

```powershell
python -m pipeline.silver.cli --dataset news --run-partition 2026-05-19 --strict
python -m warehouse.loader.cli load-silver --dataset news
dbt run --profiles-dir transform/dbt --select stg_news fact_news_article int_news_sentiment_daily mart_stock_news_signal
```

```sql
-- psql: so sánh volume
SELECT COUNT(*) FROM silver.news;
SELECT COUNT(*) FROM gold.fact_news_article;
SELECT COUNT(*) FROM gold.mart_stock_news_signal;
```

### 14.4. Tại sao Silver nhiều hơn Gold articles?

| Nguyên nhân | Giải pháp |
|---|---|
| Thiếu `published_date` hoặc `title` | Sửa parser HTML/RSS; kiểm Bronze |
| Chưa `dbt run` sau load | Chạy `fact_news_article` |
| Chỉ aggregate, không gọi `/articles` | Dùng endpoint articles hoặc tab Tin tức trên UI |
| `days_back` nhỏ | Tăng lên 30 khi backfill |

---

## 12. Runbook end-to-end

```powershell
# 1. Bronze
@'
from ingestion.unstructured_data import NewsIngestionConfig, ingest_news
print(ingest_news(NewsIngestionConfig(days_back=30)))
'@ | python -

# 2. Silver parquet (run_partition = ngày Bronze, ví dụ hôm nay)
python -m pipeline.silver.cli --dataset news --run-partition 2026-05-19 --strict

# 3. PostgreSQL
.\warehouse\scripts\setup_db.ps1
$env:DATABASE_URL = "postgresql://stock:stock@localhost:55432/stock_pipeline"
python -m warehouse.loader.cli load-silver --dataset news

# 4. Gold (toàn project hoặc chỉ news subgraph nếu silver.price đã có)
dbt run --profiles-dir transform/dbt --select stg_news+ 
# hoặc: dbt run --profiles-dir transform/dbt
dbt test --profiles-dir transform/dbt --select stg_news mart_stock_news_signal int_news_sentiment_daily

# 5. API + UI
uvicorn backend.main:app --reload --port 8000
cd frontend; npm run dev
```

Thứ tự: **Bronze → Silver files → load `silver.news` → `dbt run` → API**. Cập nhật tin: rerun Bronze/Silver/load cho `silver.news`, rồi `dbt run` (không cần rerun Bronze nếu chỉ sửa SQL Gold).

---

## 13. Checklist vận hành

| Bước | Việc cần làm |
|---|---|
| 1. Bronze | `days_back` phù hợp; `listing.parquet` nếu match ticker; `ingest_news` |
| 2. Silver | `--run-partition` trùng ngày Bronze; `--strict` |
| 3. Warehouse | `load-silver --dataset news`; kiểm `silver.load_audit` |
| 4. Gold | `dbt run` gồm `fact_news_article`; kiểm count PG |
| 5. App | `GET /news/{symbol}/articles`; NewsPanel (bài + aggregate) |

| Tình huống | Gợi ý |
|---|---|
| Lần đầu / backfill | Bronze `days_back=30`; Silver từng `run_partition` hoặc nhiều ngày; load một lần; `dbt run` |
| Hàng ngày | Bronze `days_back=1` → Silver → load → `dbt run --select +mart_stock_news_signal +fact_news_article`; Gold structured từ `structured_daily` / `gold_full_refresh` |
| `mart_stock_news_daily` ít dòng | Bình thường: filter `ticker is not null` + aggregate; tăng match ticker ở Bronze/Silver |
| Tin không hiện theo mã | Bài không nhắc mã đó trong `ticker`/`ticker_mentions`; thử `/news/market` |
| Có silver, không có fact | Chưa `dbt run --select fact_news_article` |
| `mart_stock_daily` thiếu sentiment | Chưa có tin cùng ngày giao dịch; hoặc chưa chạy `news_daily` / `mart_stock_news_signal` |
| Rerun Bronze cùng ngày | Silver + load + `dbt run` lại |
| Lỗi unique `url` khi load | Hai `article_id` cùng URL — sửa upstream |
| Giảm tải site | Hạ `NEWS_RATE_LIMIT_RPM`, cap feed/HTML |

---

## 15. Nâng cấp luồng News (2026-06)

### 15.1. Thay đổi chính

| Thành phần | Trước | Sau |
|---|---|---|
| `stg_news` | `coalesce(ticker, ticker_mentions[1])` — 1 mã/bài | `unnest(ticker_mentions)` — mỗi `(article_id, ticker)` là 1 dòng |
| Coverage | ~154 cặp `(ticker, ngày)` từ 804 bài | ~500-1500 cặp, tùy số mã trong `ticker_mentions` |
| `fact_news_article` | Không có relevance/tier | Thêm `ticker_relevance`, `source_tier` |
| `mart_stock_news_daily` | Aggregate đơn giản, không có URL | WARNING: Deprecated, giữ backward-compat; UI/API chính chuyển sang `mart_stock_news_signal` |
| `mart_stock_news_signal` | Không có | Mới: weighted sentiment, `news_signal`, `top_articles` JSONB |
| Join với giá | `trading_date = published_date` | Join qua `mart_stock_news_signal.trading_date` đã map phiên |

### 15.2. Model mới: `mart_stock_news_signal`

- **Nguồn:** `fact_news_article` (sau explode `stg_news`)
- **Grain:** `ticker + trading_date` (phiên giao dịch)
- **Mapping cuối tuần:** Tin thứ 7/CN → phiên thứ 2; tin sau 14:30 → phiên kế tiếp
- **weighted_sentiment:** Có trọng số theo relevance (3/2/1), source tier (1.5/1.2/1.0), time-decay (half-life 48h)
- **news_signal:** `buy_signal | sell_signal | neutral` — trading signal từ `weighted_sentiment` với ngưỡng ±0.3.
- **Phân biệt label:** `sentiment_label` / `dominant_sentiment` dùng `positive | negative | neutral`; riêng `news_signal` dùng `buy_signal | sell_signal | neutral`.
- **top_articles:** JSONB chứa tối đa 3 bài quan trọng nhất kèm title + url + sentiment

### 15.3. Endpoint mới/sửa

| Endpoint | Thay đổi |
|---|---|
| `GET /news/{symbol}` | Đọc `mart_stock_news_signal` thay vì `mart_stock_news_daily`; trả `top_articles` kèm URL |
| `GET /news/{symbol}/signal` | Mới — trả signal nhanh nhất cho header UI |
| `GET /news/{symbol}/articles` | Thêm filter `?relevance=title\|summary\|body` |

### 15.4. Lưu ý vận hành

- `stg_news` giờ có grain `(article_id, ticker)` — không còn unique trên `article_id`
- `dbt test` đã đổi sang unique composite `(article_id, ticker)`
- Ngưỡng `news_signal` (±0.3) cần calibrate sau khi có đủ dữ liệu thực tế
- `mart_stock_news_daily` vẫn giữ lại để backward-compat; WARNING: deprecated và sẽ bỏ sau khi UI/API ổn định

---

## 16. Current News UI/API Behavior (2026-06-03)

This section supersedes older notes that describe stock news only as
`ticker + published_date` aggregates or say stock detail reads only
`mart_stock_news_daily`.

### 16.1. Current Gold sources

| Source | Grain | Used by |
|---|---|---|
| `gold.fact_news_article` | `article_id + ticker` | Main news archive, article search/filter, `/news/articles`, `/news/{symbol}/articles`. |
| `gold.mart_stock_news_signal` | `ticker + trading_date` | Stock-detail news signal panel and `/news/{symbol}` / `/news/{symbol}/signal`. |
| `gold.mart_stock_news_daily` | `ticker + published_date` | WARNING: Deprecated backward-compatible simple aggregate; not the main stock-detail UI source. |

`stg_news` now explodes one article into multiple ticker rows when
`ticker_mentions` contains multiple symbols. Because of that,
`fact_news_article` is unique by `(article_id, ticker)`, not by `article_id`
alone.

### 16.2. Date logic

- Article archive filters use `fact_news_article.published_date`.
- Stock-detail signal filters use `mart_stock_news_signal.trading_date`.
- `mart_stock_news_signal.trading_date` maps articles to trading sessions:
  weekend articles move to the next trading session, and articles after the
  cutoff move to the next session instead of the same calendar date.

### 16.3. Sentiment fields

| Field | Meaning |
|---|---|
| `avg_sentiment_score` | Simple average of keyword sentiment scores for articles in the ticker/session group. |
| `weighted_sentiment` | Weighted average. Higher weights are given to stronger ticker relevance (`title` > `summary` > `body`), higher source tier, and fresher articles. |
| `dominant_sentiment` | Majority label by article count: `positive`, `negative`, or `neutral`. |
| `news_signal` | Trading-session signal from `weighted_sentiment`: `buy_signal` when `>= 0.3`, `sell_signal` when `<= -0.3`, otherwise `neutral`. |

The current sentiment method is keyword-based (`keyword_v1`), not an ML model.
Positive/negative/neutral labels belong to `sentiment_label` and `dominant_sentiment`; they come from keyword score thresholds in the news Silver/dbt flow. `news_signal` is intentionally a different field and only emits `buy_signal`, `sell_signal`, or `neutral`.

### 16.5 Exact formula (current SQL)

In `mart_stock_news_signal`, each article contributes an `article_weight`:

`article_weight = relevance_weight * source_weight * time_decay`

- `relevance_weight`:
  - `title` = `3.0`
  - `summary` = `2.0`
  - `body` = `1.0`
- `source_weight`:
  - `source_tier = 1` -> `1.5`
  - `source_tier = 2` -> `1.2`
  - otherwise -> `1.0`
- `time_decay`:
  - `exp(-age_seconds / (48 * 3600))`
  - where `age_seconds = extract(epoch from (current_timestamp - published_at))`

Grouped by `ticker + trading_date`:

- `avg_sentiment_score = round(avg(sentiment_score), 4)`
- `weighted_sentiment = round(sum(sentiment_score * article_weight) / nullif(sum(article_weight), 0), 4)`

Signal thresholds:

- `weighted_sentiment >= 0.3` -> `buy_signal`
- `weighted_sentiment <= -0.3` -> `sell_signal`
- otherwise -> `neutral`

`dominant_sentiment` is count-based (not weighted):

- `positive` if `positive_count > negative_count`
- `negative` if `negative_count > positive_count`
- otherwise `neutral`

### 16.6 News archive vs stock news panel

- Main news archive (`/news`, API `/news/articles`) reads `gold.fact_news_article` and can show preview/full text from `body_text` when available.
- Stock-detail news panel (`/stock/:symbol`, API `/news/{symbol}` and `/news/{symbol}/signal`) reads `gold.mart_stock_news_signal`, which is trading-session aggregate data plus compact `top_articles` metadata (title/url/sentiment/relevance/weight), not full article body text.

### 16.4. Article content in UI

- Stock-detail news signal uses `mart_stock_news_signal.top_articles`. The JSON currently stores compact article metadata such as title, URL, sentiment, publish time, relevance, and weight. It is meant for quick context, not full article reading.
- The main news archive reads `fact_news_article`, which includes `title`, `summary`, and `body_text`.
- The archive shows a Preview action only for rows where `body_text` exists. Clicking Preview opens the full `body_text`. Articles without body text skip the preview action.
