# Ingestion

Thư mục này chứa 3 luồng ingest dữ liệu vào tầng raw của data lake. Mỗi luồng có cấu hình, notebook/script điều phối và README chi tiết trong thư mục con tương ứng.

## Tóm tắt các luồng

| Luồng | Dữ liệu | Nguồn chính | Xử lý chính | Output raw |
|------|--------|-------------|-------------|------------|
| [Structure Data](structure_data/README.md) | Giá OHLCV, chỉ số, listing, company overview, financial ratio, price board | API `vnstock` | Đọc `IngestionConfig`, đăng ký API key, gọi API có rate limit/retry, QC vận hành, fallback source, gắn metadata tối thiểu, ghi Parquet raw-ish | `data-lake/raw/Structure_Data/` |
| [Semi-structured Data](semi_structure_data/README.md) | PDF báo cáo tài chính năm | HNX live, `hnx_sample.json`, `hnx_urls.csv` | Crawl danh sách công bố, lấy PDF đính kèm, lọc/classify tài liệu, chọn bản canonical, tải PDF và ghi metadata | `data-lake/raw/Semi_Structure_Data/bctc_annual_pdf*` |
| [Unstructured Data](unstructured_data/README.md) | Tin tức chứng khoán/doanh nghiệp | RSS và HTML từ `sources.yaml` hoặc config | Fetch RSS/HTML, parse nội dung, lọc theo `days_back`, match ticker, dedupe, validate, ghi Parquet | `data-lake/raw/Unstructure_Data/news/` |

## Vai trò từng luồng

- **Structure Data** là luồng dữ liệu có cấu trúc, ưu tiên dùng cho phân tích định lượng và làm nguồn tham chiếu như danh sách mã, giá, chỉ số và thông tin doanh nghiệp. Bronze giữ dữ liệu gần nguồn nhất có kèm metadata vận hành; chuẩn hóa analytics-ready nằm ở `pipeline/silver/`.
- **Semi-structured Data** lưu trữ PDF BCTC từ HNX cùng metadata kiểm chứng. Luồng này hiện tập trung vào crawl/download, không parse OCR/PDF sang bảng phân tích.
- **Unstructured Data** thu thập tin tức từ RSS/HTML, chuẩn hóa schema bài viết và có thể gắn ticker dựa trên danh sách mã cấu hình hoặc listing đã ingest.

## Ghi chú vận hành

- Các luồng đều ghi dữ liệu raw dạng Parquet hoặc file gốc. Riêng Structure OHLCV (`price`, `index`) partition theo `year=<YYYY>/month=<MM>`; các snapshot/news/PDF metadata còn lại dùng partition snapshot, timestamp hoặc ngày chạy.
- Silver structured hiện xử lý đủ 6 dataset: `price`, `index_price`, `listing`, `company`, `financial_ratio`, `price_board`.
- Silver news và BCTC metadata cũng đã có transformer trong `pipeline/silver/`; news cần truyền `--run-partition`, BCTC hiện dừng ở metadata PDF, không OCR/parse bảng.
- Cơ chế retry, rate limit và một số helper dùng chung nằm trong `ingestion.common`.
- Nếu cần chạy theo notebook, dùng các notebook manager trong thư mục `ingestion/`.
- Thông tin cấu hình chi tiết, biến môi trường và layout output cụ thể nằm trong README của từng luồng.
