# Project documentation index

Tài liệu trong thư mục này được chia theo 2 lớp:

1. **Tài liệu điều hướng** — giúp đọc nhanh, hiểu chỗ nào là nguồn chuẩn.
2. **Tài liệu chi tiết theo luồng** — mô tả cụ thể từng pipeline, contract, và lineage.

## Nên đọc theo thứ tự

1. `../README.md` — tổng quan kiến trúc, cách chạy, và runbook chính.
2. `../pipeline_infor.md` — bản tổng hợp luồng dữ liệu end-to-end, tham số vận hành, và mapping API/UI.
3. `Overview_Flow/Structure_data_flow.md` — luồng structured data.
4. `Overview_Flow/News_data_flow.md` — luồng news.
5. `Overview_Flow/BCTC_data_flow.md` — luồng BCTC PDF.
6. `Output & Param/dbt_outputs_and_lineage.md` — contract Silver → Gold và endpoint lineage.
7. `Output & Param/parameters.md` — tham số env / config / CLI / DAG.

## Nguồn tài liệu hiện tại

| File | Mục đích |
|---|---|
| `../README.md` | Trang chủ của dự án, hướng dẫn chạy và giới thiệu tổng quan |
| `../pipeline_infor.md` | Single-entry summary cho toàn bộ luồng dữ liệu |
| `Overview_Flow/Structure_data_flow.md` | Tài liệu chi tiết structured flow |
| `Overview_Flow/News_data_flow.md` | Tài liệu chi tiết news flow |
| `Overview_Flow/BCTC_data_flow.md` | Tài liệu chi tiết BCTC flow |
| `Output & Param/dbt_outputs_and_lineage.md` | Column catalog, Gold lineage, API/UI mapping |
| `Output & Param/parameters.md` | Bảng tham số vận hành 4 lớp |

## Ghi chú dọn dẹp

- `Docs/codebase-audit/codebase-audit.md` là bản tổng hợp phục vụ review, không phải nguồn chuẩn chính.
- `SLIDE_PROJECT.pdf` là tài liệu trình bày; giữ riêng nếu cần cho báo cáo/defense.
- `REPORT_PAPER.pdf` là tài liệu báo cáo trình bày đầy đủ từ khảo sát, thiết kế và triển khai.
