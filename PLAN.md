# Báo Cáo Trạng Thái & Kế Hoạch Tiếp Theo

## 1. Tóm Tắt Hiện Trạng

Đã đối chiếu `MEDALLION_PLAN.md` với trạng thái thực tế: 8 dataset Silver chính đã có Parquet và đã load khớp vào PostgreSQL/TimescaleDB. Phần còn thiếu lớn nhất là dbt Gold, FastAPI, frontend và Airflow. Hai bảng `silver.bctc_facts` và `silver.price_indicator` đang tồn tại trong DB nhưng đều 0 dòng và không nên nằm trên đường chạy Silver MVP.

### Bảng 1 — Trạng Thái Silver Theo Dataset

| Dataset | Parquet rows | DB rows | Khớp? | Partition mới nhất | Ghi chú |
|---|---:|---:|---|---|---|
| price | 62,339 | 62,339 | Có | `trading_date=2026-05-18` | Loaded, audit success |
| index_price | 6,234 | 6,234 | Có | `trading_date=2026-05-18` | Loaded, audit success |
| listing | 1,535 | 1,535 | Có | `current` | Loaded, audit success |
| company | 50 | 50 | Có | `current` | Loaded, audit success |
| financial_ratio | 15,282 | 15,282 | Có | `period_type=quarter/year=2026` | Cần fix watermark full timestamp |
| price_board | 50 | 50 | Có | `trading_date=2026-05-18` | Cần chốt latest snapshot/day |
| news | 804 | 804 | Có | `date=2026-05-19` | Chỉ có 1 ngày news hiện tại |
| bctc_pdf_meta | 1,458 | 1,458 | Có | `date=2026-05-14` | Metadata/search only |

### Bảng 2 — Trạng Thái Module Theo MEDALLION_PLAN

| Module | Mô tả | Trạng thái | % | Còn thiếu gì |
|---|---|---|---:|---|
| M1 Bronze Ingestion | Thu thập 3 luồng | Đã có output chính | 80% | `_runs`, runbook, xác nhận universe 50 ticker |
| M2 Silver Structured | 6 dataset OHLCV/dim | Parquet + DB đã khớp | 80% | Fix watermark, fix price_board grain, `_runs`, thêm tests |
| M3 Silver News | Chuẩn hóa tin tức | Silver + DB + tests đã có | 75% | Backfill thêm ngày news, `_runs`, reprocess all partitions |
| M4 Silver BCTC | Metadata PDF | Silver + DB đã có | 80% | Test transformer BCTC, `_runs`, giữ metadata-only |
| M5 Warehouse Loader | Load 8 bảng Silver | Đã load đủ 8 bảng | 90% | Cleanup schema lệch plan, load theo partition nếu cần |
| M6 dbt Gold | Staging/intermediate/marts | Chưa có dbt model SQL | 5% | Tạo project, profiles, models, tests |
| M7 Airflow | 3 DAG | Chỉ có README | 5% | 3 DAG manual |
| M8 FastAPI | Read-only Gold APIs | Skeleton | 5% | App, routers, schemas, endpoints |
| M9 Frontend | Dashboard/detail | Skeleton | 5% | React app thực tế |
| M10 Tests/Docs | Tests/runbook/demo | 58 tests pass khi set `PYTHONPATH` | 55% | Fix pytest config, integration test, docs demo |

### Bảng 3 — Hai Bảng Đặc Biệt

| Bảng | Có trong plan? | Vai trò đúng theo plan | Hành động cần làm |
|---|---|---|---|
| `silver.bctc_facts` | Không trong MVP | Future scope nếu sau này OCR/parse PDF thành facts | Không load/query/API; deprecate khỏi đường chạy demo |
| `silver.price_indicator` | Có ý tưởng nhưng sai layer | Phải là dbt intermediate `int_price_indicator`, không phải Silver table | Không populate bảng này; tính indicator trong dbt |

## 2. Kế Hoạch Hành Động Ưu Tiên

### P0a — Fix Kỹ Thuật Silver Trước

Mục tiêu: sửa data correctness trước khi làm housekeeping.

- Sửa `financial_ratio` watermark từ date-only sang full `snapshot_date` timestamp.
- Đảm bảo incremental check dùng `snapshot_date > last_snapshot_date`.
- Cập nhật `price_board` transformer theo grain `symbol + trading_date`.
- Nếu có nhiều snapshot cùng ngày, giữ bản có `snapshot_at` mới nhất.
- Chạy lại tests liên quan `financial_ratio` và `price_board`.

Acceptance: transformer không tạo duplicate key; partition output vẫn load khớp DB; tests liên quan pass.

### P0b — Infra, Config, Audit Và Schema Cleanup

Mục tiêu: làm sạch nền tảng sau khi logic Silver đã đúng.

- Thêm cấu hình để `pytest -q` chạy trực tiếp từ repo root, không cần set tay `PYTHONPATH`.
- Thêm `_runs` log cho Silver structured/news/BCTC theo contract trong plan.
- Cleanup DDL theo hướng MVP: không coi `silver.bctc_facts` và `silver.price_indicator` là bảng active.
- Không drop dữ liệu tự động; vì hai bảng hiện 0 rows, có thể deprecate trong DDL/demo reset thay vì dùng trong pipeline.
- Cập nhật README/runbook để ghi rõ `price_indicator` thuộc dbt `int_price_indicator`.

Acceptance: `pytest -q` pass trực tiếp; 8 bảng Silver chính vẫn load idempotent; tài liệu không còn khuyến khích dùng 2 bảng đặc biệt trong Silver.

### P1 — Xây dbt Gold MVP Cho Structured

Mục tiêu: dựng dbt đúng vị trí, đúng kết nối, không để Codex đoán config.

- dbt project đặt tại `transform/dbt/`.
- Dùng profile name `stock_pipeline`, target `dev`, schema output `gold`.
- Với local demo, tạo `transform/dbt/profiles.yml` và chạy dbt bằng `--profiles-dir transform/dbt`.

`profiles.yml` cần có:

```yaml
stock_pipeline:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 55432
      user: stock
      password: stock
      dbname: stock_pipeline
      schema: gold
```

- Tạo staging cho 6 bảng structured.
- Tạo `int_price_indicator` trong dbt từ `silver.price`.
- Tạo marts: `mart_stock_daily`, `mart_company_profile`, `mart_market_overview`.
- Không dùng hoặc populate `silver.price_indicator`.

Acceptance: `dbt run --profiles-dir transform/dbt` và `dbt test --profiles-dir transform/dbt` pass; marts structured có dữ liệu tới `2026-05-18`.

### P2 — News Và BCTC Vào Gold

Mục tiêu: đưa unstructured/semi-structured vào Gold nhưng xử lý rõ hạn chế dữ liệu news.

- Trước khi chạy dbt news models, backfill Bronze -> Silver news cho tất cả partition hiện có hoặc mở rộng ingest news thêm nhiều ngày.
- Nếu vẫn chỉ có partition `2026-05-19`, ghi rõ mart news chỉ phục vụ smoke test, không đủ đẹp cho demo trend.
- Tạo staging `stg_news`, `stg_bctc_pdf_meta`.
- Tạo `int_news_sentiment_daily` theo grain `ticker + published_date`.
- Tạo `mart_stock_news_daily` và join sentiment vào `mart_stock_daily`.
- Tạo `mart_bctc_documents` từ metadata PDF; không dùng `silver.bctc_facts`.

Acceptance: news mart có dữ liệu sau backfill; BCTC mart có dữ liệu từ 1,458 metadata; query theo ticker/năm/kỳ trả kết quả.

### P3 — FastAPI Read-Only Trên Gold

- Implement FastAPI app, DB connection config, Pydantic response models.
- Endpoints: `/health`, `/tickers`, `/market/overview`, `/companies/{symbol}`, `/prices/{symbol}`, `/indicators/{symbol}`, `/financials/{symbol}`, `/news/{symbol}`, `/bctc/{symbol}`, `/bctc/{symbol}/file/{doc_id}`.
- API chỉ đọc schema `gold`.
- BCTC file endpoint dùng `FileResponse` nếu `pdf_path` local tồn tại, nếu không redirect `url_pdf`.

Acceptance: `/tickers` trả 50 ticker; endpoints chính có filter/pagination; mở PDF hoạt động.

### P4 — Frontend Demo

- Tạo dashboard và ticker detail page thực tế.
- Dashboard: search ticker, market overview, top gainers/losers, latest news.
- Detail: profile, price chart, indicators, news, financial ratios, BCTC tab.
- Kết nối toàn bộ qua FastAPI, không đọc DB trực tiếp.

Acceptance: search điều hướng được; chart/news/BCTC render từ API; app có loading/error/empty states.

### P5 — Airflow, Integration Tests, Docs

- Tạo 3 DAG manual: structured daily, news daily, BCTC quarterly.
- Thêm integration test Bronze sample -> Silver -> Warehouse -> Gold cho 1-2 ticker.
- Viết `docs/RUNBOOK.md`, `docs/DEMO.md`, data dictionary.
- Cập nhật README sau khi Gold/API/frontend hoàn tất.

Acceptance: DAG import không lỗi; demo có chuỗi lệnh rõ; unit/integration/dbt tests pass.

## 3. Test Plan

- P0a: tests cho `financial_ratio` watermark và `price_board` latest snapshot/day.
- P0b: `pytest -q` pass trực tiếp từ repo root.
- Warehouse: load `--dataset all` hai lần, row count không tăng duplicate.
- dbt: `dbt run/test --profiles-dir transform/dbt`.
- API: smoke test tất cả endpoints chính.
- Frontend: kiểm tra dashboard/detail trên desktop và mobile.

## 4. Assumptions

- Universe 50 ticker hiện tại là demo universe cho MVP.
- Local DB dùng `localhost:55432`, user/password `stock/stock`, database `stock_pipeline`.
- `transform/dbt/` là dbt root chính thức.
- BCTC MVP chỉ search/view metadata PDF; không parse facts.
- News hiện chỉ có 1 ngày dữ liệu nên phải backfill trước khi dùng cho demo sentiment trend.
