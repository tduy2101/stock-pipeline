---
name: viet-bao-cao-do-an-latex
description: >
  BẮT BUỘC đọc trước khi viết/chỉnh/sửa bất kỳ đoạn LaTeX nào trong báo cáo
  đồ án stock-pipeline. Kích hoạt khi: viết mục/chương báo cáo, chỉnh văn phong
  đồ án, sửa LaTeX cho phù hợp hội đồng, hoặc user nhắc RULE_viet_bao_cao.
  Đầu ra là báo cáo đồ án kỹ thuật dữ liệu: tiếng Việt rõ ràng, trung lập,
  cô đọng; ưu tiên hình và danh sách khi đủ diễn đạt; chỉ dùng bảng khi thật sự
  cần so sánh nhiều cột — KHÔNG phải README, KHÔNG phải luận văn toán/khoa học
  thuần văn; không đưa path/file/hàm vào đoạn phân tích chính.
---

# Skill: Viết Báo Cáo Đồ Án LaTeX — stock-pipeline

---

## 0. Quy tắc bắt buộc cho Agent (đọc trước tiên)

### 0.1. Hai lớp nội dung — không nhầm vai trò

| Lớp | Đối tượng đọc | Cách viết |
|---|---|---|
| **Thân báo cáo** | Hội đồng, giảng viên | Tiếng Việt trung lập, dễ đọc; đoạn ngắn + hình/list trước; bảng khi cần so sánh nhiều cột; nêu rõ *vai trò, luồng, grain, chính sách* |
| **Phụ lục / bảng kỹ thuật / lstlisting** | Người cần đối chiếu mã | Được phép tên file, path, hàm, schema — có `\caption` |

**Nguyên tắc vàng:** Đọc codebase để đúng sự thật; **không** paste codebase vào văn xuôi.

### 0.2. Không lạm dụng chi tiết kỹ thuật trong văn xuôi (`\section`, `\subsection`, đoạn phân tích)

Không đưa chi tiết triển khai thô vào đoạn phân tích nếu chi tiết đó không trực
tiếp phục vụ lập luận học thuật. Các nhóm dưới đây cần tránh trong thân báo cáo,
đặc biệt ở Chương 1--3:

- Đường dẫn repo: `data-lake/...`, `ingestion/...`, `docker/airflow/...`
- Tên file / notebook: `ingest_structure_data_manager.ipynb`, `config.py`, `tasks.py`
- Tên hàm / class / biến: `ingest_prices`, `IngestionConfig`, `_watermark.json`, `ON CONFLICT DO UPDATE`
- Tên DAG / task Airflow dạng mã: `structured_daily`, `gold_full_refresh`, `bctc_quarterly`
- Endpoint HTTP: `GET /prices/{symbol}`
- Tham số env/CLI dạng mã: `STRUCTURED_MAX_TICKERS`, `days_back=30`, `latest_partitions=7`
- Câu mô tả call stack: "Hàm X gọi Y rồi gọi Z"
- Đoạn meta mô tả hình thay vì câu dẫn học thuật: "Hình X: sáu khối dataset…"

Tên dataset, schema, bảng, mart hoặc cột kỹ thuật **được phép dùng có kiểm
soát** khi mục đang định nghĩa hợp đồng dữ liệu, mô tả mô hình Gold, trình bày
triển khai ở Chương 4 hoặc lập bảng đối chiếu kỹ thuật. Không liệt kê dày đặc
các tên này trong một đoạn văn; nếu có nhiều hơn ba tên, chuyển sang bảng hoặc
phụ lục.

### 0.3. Được phép giữ trong văn xuôi (tên riêng công nghệ)

Chỉ khi là tên sản phẩm / chuẩn đã phổ biến, **không** bọc `\texttt{}`:

PostgreSQL, TimescaleDB, dbt, Airflow, FastAPI, React, Vite, TypeScript, Python,
Parquet, vnstock, Docker, pytest, RSS, HTML, PDF, OHLCV, HOSE, HNX, ELT, ETL.

Các khái niệm kiến trúc đã giải thích bằng tiếng Việt ở chương trước: Bronze,
Silver, Gold, Medallion, grain, watermark, DAG (khi nói *đồ thị điều phối* chung).

### 0.4. Checklist trước khi trả LaTeX cho user

Agent phải tự rà từng mục; nếu vi phạm thì sửa trước khi gửi:

1. Không còn path / tên file / tên hàm trong đoạn văn phân tích.
2. Không còn `\_` escape tên kỹ thuật trong văn xuôi.
3. Mỗi bảng: caption đặt dưới bảng, cách bảng khoảng `2pt`; `\caption` **trước** `\label`; có câu dẫn `Bảng~\ref{...}` phía trước.
4. Mỗi hình: `\caption` tiếng Việt (không "Tech Stack" thuần Anh); có câu dẫn `Hình~\ref{...}`.
5. Không dùng `\textbf{}`, `\textit{}`, `\texttt{}` để trang trí. `\textbf{}`
   chỉ dùng cho nhãn ngắn đầu dòng/đầu đoạn hoặc tiêu đề cột; `\texttt{}` chỉ
   dùng trong bảng kỹ thuật, phụ lục hoặc `lstlisting`.
6. Dẫn chiếu chương/mục: `Mục~\ref{sec:...}`, `Chương~\ref{...}` — không hardcode "Mục 2.3".
7. Số liệu snapshot có mốc thời gian: "tính đến ngày YYYY-MM-DD".
8. Nội dung kỹ thuật đã đối chiếu code hoặc Docs (ưu tiên code khi mâu thuẫn với README).
9. Không tự bịa tham số; thiếu thông tin dùng `[CẦN XÁC NHẬN: …]` trong nháp nội bộ, không trong bản chính gửi user.
10. Mục có từ ba thực thể trở lên: ưu tiên `itemize` hoặc hình; chỉ dùng bảng
    khi cần so sánh theo nhiều cột (mục 2.4.1).
11. Không tạo bảng chỉ để liệt kê lại nội dung đã có trong văn xuôi hoặc
    `itemize`; không lặp cùng một bảng ở nhiều mục — dùng `\ref{}`.
12. Không để bảng/hình tự đứng một mình: trước đó có câu dẫn; sau đó có 1--3 câu nhận xét, diễn giải ý nghĩa hoặc chuyển mục. Không lặp lại từng dòng của bảng/hình.
13. Không còn cụm văn hoa/mẫu luận văn (mục 1.1): ``đóng vai trò then chốt'', ``tạo điều kiện thuận lợi'', ``mang ý nghĩa quan trọng''…
14. Mỗi `\section` có đủ: câu mở mục, phần giải thích chính, bảng/hình/list nếu phù hợp, và câu nhận xét hoặc chuyển ý.
15. Chương 1: phần cơ sở lý thuyết phải trình bày định nghĩa chung trước; hình trong phần này là hình khái niệm/học thuật, không phải sơ đồ thiết kế riêng của dự án.
16. Mỗi `\subsection` lý thuyết: tối đa **một** bảng so sánh; tránh chuỗi nhiều bảng cùng loại (ETL vs ELT rồi lại batch vs streaming rồi lại lý do batch).
17. Chương 3: mỗi mục thiết kế phải nêu bài toán/ràng buộc, quyết định thiết kế, lý do lựa chọn và hợp đồng dữ liệu; không trôi sang hướng dẫn chạy chương trình.

### 0.5. Mẫu output khi user gửi LaTeX cần "sửa cho phù hợp báo cáo"

1. Giữ nguyên `\label{}` hiện có nếu user đã dùng trong báo cáo.
2. Thay path/tên kỹ thuật bằng diễn đạt tiếng Việt theo bảng mục 4.4.
3. Sửa caption/label bảng sai thứ tự.
4. Ghi file vào `Docs/latex/ch{N}_revised.tex` hoặc trả trực tiếp block LaTeX — không chỉ liệt kê gợi ý mà không sửa.

---

## 1. Nguyên tắc ngôn ngữ và văn phong

### 1.0. Ba kiểu viết — chọn đúng kiểu đồ án kỹ thuật dữ liệu

| Kiểu | Đặc điểm | Áp dụng? |
|---|---|---|
| README / tài liệu nội bộ | Bullet dày, path, tên hàm, lệnh CLI | Không — chỉ phụ lục |
| Luận văn toán / khoa học thuần văn | Đoạn dài, bị động, định nghĩa, lập luận trừu tượng | Không — tránh |
| **Đồ án kỹ thuật dữ liệu** | Câu ngắn, bảng/sơ đồ, mô tả pipeline và thiết kế cụ thể | **Có — mục tiêu** |

**Công thức một mục đầy đủ:** câu mở nêu mục tiêu → định nghĩa/bối cảnh hoặc
vấn đề → diễn giải chính → **bảng / hình / list** khi có nhiều thực thể hoặc
cần so sánh → 1--3 câu nhận xét, kết luận hoặc `\ref{}` sang mục khác. Bảng
và hình giúp cô đọng thông tin, nhưng không thay thế hoàn toàn lập luận trong
văn xuôi.

Báo cáo thuộc loại **đồ án kỹ thuật dữ liệu**: trình bày pipeline, kiến trúc
phân tầng và kết quả triển khai. Giọng **trung lập, trực tiếp, dễ hiểu** —
đọc xong biết hệ thống làm gì, không cần diễn giải vòng vo.

**Phong cách tham chiếu từ mẫu DATN_HN_ver1.pdf:** dùng mẫu này để học nhịp
trình bày, không sao chép nội dung. Mỗi chương nên có đoạn dẫn nhập nêu mục
tiêu chương và các nhóm nội dung chính. Mỗi mục chính cần đi từ bối cảnh hoặc
khái niệm chung, sau đó mới phân tích nội dung cụ thể của đề tài. Bảng và hình
được dùng để hệ thống hóa ý, nhưng luôn phải có câu dẫn trước và câu phân tích
sau. Đoạn văn có thể dài hơn khi đang định nghĩa lý thuyết, miễn là mỗi đoạn
vẫn tập trung vào một ý rõ ràng.

Mẫu đồ án có thể dùng nhãn ngắn ở đầu đoạn hoặc đầu dòng để chia ý, ví dụ dạng
``Bối cảnh dữ liệu:'', ``Nhu cầu thực tiễn:'' hoặc ``Lý do chọn đề tài:''.
Được áp dụng cách này khi mục có nhiều ý ngang hàng, nhưng không biến toàn bộ
báo cáo thành danh sách nhãn in đậm.

Riêng Chương 1, phần cơ sở lý thuyết phải viết theo thứ tự: định nghĩa chung,
đặc điểm chính, so sánh hoặc vai trò, ví dụ khái quát, sau đó mới liên hệ ngắn
với đề tài. Không đưa quyết định thiết kế, tên DAG, tên mart, sơ đồ kiến trúc
dự án hoặc chi tiết triển khai vào hình lý thuyết của Chương 1.

**Giọng văn:**
- Dùng câu chủ động khi nói thiết kế: ``hệ thống tách ba luồng'', ``đề tài
  chọn ELT theo lô''.
- Dùng bị động ngắn khi nói dữ liệu: ``dữ liệu được chuẩn hóa ở Silver'' —
  tối đa một câu bị động liên tiếp.
- Không dùng ngôi thứ nhất trừ Mở đầu/Kết luận.
- Ưu tiên động từ cụ thể: *thu thập, chuẩn hóa, nạp, build, tra cứu* thay
  vì *thực hiện, tiến hành, đảm nhiệm, hướng tới*.

**Thuật ngữ:** Tiếng Việt cho nghiệp vụ; giữ tên công nghệ bắt buộc
(PostgreSQL, dbt, Airflow…). Không song ngữ trong ngoặc. Không in đậm khi
giới thiệu khái niệm lần đầu nếu chỉ nhằm trang trí.

**Câu:** Một câu một ý; ưu tiên khoảng **12--25 từ**. Câu dài hơn vẫn chấp
nhận được khi định nghĩa lý thuyết, nhưng trên 30 từ nên tách câu hoặc chuyển
sang bảng/list nếu đang liệt kê nhiều ý.

**Đoạn văn:** **2--4 câu** mỗi đoạn. Không mở mục bằng định nghĩa từ điển khô
cứng kiểu ``X là một…'' rồi bỏ qua giải thích; nếu là mục lý thuyết, định
nghĩa phải đi kèm đặc điểm, vai trò hoặc so sánh. Không kết đoạn bằng câu sáo
``Nhìn chung…'', ``Tóm lại toàn bộ mục trên…'' nếu đã có bảng/list ngay trước.

**Cô đọng:** Mỗi `\section` trả lời: *nói gì — tại sao — cụ thể ra sao*.
Bỏ từ đệm và lặp tiêu đề.

**Hạn chế ngoặc đơn:** Không chèn path, endpoint, thuật ngữ Anh dư.

### 1.1. Cụm từ và mẫu câu nên tránh (văn quá học thuật)

Không dùng (trừ khi thật sự cần, tối đa một lần mỗi chương):

| Tránh | Thay bằng |
|---|---|
| ``đóng vai trò then chốt / quan trọng'' | ``phục vụ…'' / ``dùng để…'' |
| ``tạo điều kiện thuận lợi cho'' | ``cho phép'' / ``hỗ trợ'' |
| ``mang ý nghĩa / tính chất'' | nêu thẳng ý nghĩa kỹ thuật |
| ``trong bối cảnh / trong phạm vi'' (lặp) | bỏ hoặc thay ``với đề tài này'' |
| ``không thể coi là / không còn là'' | ``không phải'' |
| ``Song song với / Cùng với sự phát triển'' | câu vào thẳng vấn đề |
| ``Vì vậy, / Do đó,'' ở đầu mọi đoạn | chỉ dùng khi có quan hệ nhân-quả thật |
| ``Nhu cầu ngày càng trở nên quan trọng'' | ``cần…'' |
| ``được thiết kế nhằm mục đích'' | ``để'' + động từ |
| ``nói cách khác / tức là'' | viết lại một cách duy nhất |

**Mẫu mở mục nên dùng:**
- ``Mục này trình bày…'' (1 câu)
- ``Hệ thống gồm…'' / ``Pipeline gồm…'' → bảng/list
- ``Bảng~\ref{…} liệt kê…'' → bảng

**Mẫu mở mục tránh:**
- ``Trong thực tế vận hành, …''
- ``Một trong những thách thức lớn nhất…''
- Đoạn 5--8 câu giới thiệu chung chung trước khi vào nội dung kỹ thuật

### 1.2. Tỷ lệ văn / bảng / danh sách theo chương

| Chương | Văn xuôi (tối đa) | Bảng / hình / danh sách |
|---|---|---|
| **1** — Tổng quan, lý thuyết | 55--65\% | Định nghĩa chung, bảng so sánh, hình khái niệm; liên hệ đề tài ngắn |
| **2** — Khảo sát, yêu cầu | 35--45\% | Đặc trưng nguồn, yêu cầu chức năng/phi chức năng |
| **3** — Thiết kế | 35--45\% | Grain, partition, so sánh ba luồng, sơ đồ |
| **4** — Triển khai | 25--35\% | Mart, endpoint, lịch DAG, `lstlisting` |
| **5** — Đánh giá | 40--50\% | Kết quả kiểm thử, tiêu chí, hạn chế (list) |

Quy tắc nhanh cho **bảng vs danh sách vs hình** (xem mục 2.4.1):

- Liệt kê cùng loại, ít thuộc tính → `itemize` / `enumerate`.
- So sánh **≥ 2 thuộc tính song song** (ví dụ ETL vs ELT theo 3--4 tiêu chí) → bảng.
- Luồng, kiến trúc, quan hệ → hình; không chuyển thành bảng chỉ để ``cho đủ bảng''.
- Nội diễn đạt được trong 1--2 câu → **không** tạo bảng.

---

## 2. Quy tắc định dạng LaTeX

### 2.1. Nhấn mạnh văn bản — hạn chế tối đa

Báo cáo ưu tiên nội dung rõ ràng, không trang trí bằng định dạng. Mặc định:
không dùng định dạng nhấn mạnh chỉ để làm nổi bật cảm tính. Có thể dùng nhãn
ngắn in đậm ở đầu đoạn hoặc đầu dòng khi nó giúp chia rõ các nhóm ý như phong
cách đồ án mẫu.

| Lệnh | Nguyên tắc |
|---|---|
| `\textbf{}` | Dùng cho tiêu đề cột bảng, nhãn ngắn đầu đoạn hoặc nhãn đầu dòng trong `itemize`. Không in đậm tên lớp Bronze/Silver/Gold, tên mart, tên dataset chỉ để trang trí. |
| `\textit{}` | Tránh trong văn xuôi. Không dùng cho đường dẫn, tên file,
tên module. |
| `\texttt{}` | Chỉ dùng trong `lstlisting`, bảng kỹ thuật, phụ lục, hoặc khi tên riêng không thể thay bằng tiếng Việt mà không gây nhầm lẫn. Không bọc hàng loạt tên dataset, endpoint, cột, biến trong văn xuôi. |

**Quy tắc thay thế trong văn xuôi** (xem bảng đầy đủ tại mục 4.4):

| Tránh viết | Nên viết |
|---|---|
| `\texttt{mart\_stock\_daily}` | bảng tổng hợp giá và chỉ báo theo ngày |
| `\textit{pipeline/silver/}` | tầng biến đổi Silver |
| `GET /prices/\{symbol\}` | API tra cứu lịch sử giá theo mã |
| `data-lake/silver/<dataset>/\_runs.jsonl` | cơ chế ghi nhật ký vận hành sau mỗi lần biến đổi |
| `structured\_daily` / `structured\_monthly` | DAG hàng ngày / DAG hàng tháng (luồng có cấu trúc) |
| `ingest\_structure\_data\_manager.ipynb` | notebook điều phối thu thập và backfill |
| `listing, price, index\_price, …` | tám tập dữ liệu; liệt kê bằng tên nghiệp vụ tiếng Việt |
| `read-only` / `OLTP` | chỉ đọc / ứng dụng giao dịch nghiệp vụ |
| `Data Engineering` (lặp nhiều) | kỹ thuật dữ liệu |
| `data lake` | kho dữ liệu hồ |
| `incremental load` | cập nhật gia tăng |
| `snapshot` | ảnh chụp trạng thái |
| `(\textit{batch ELT})` | xử lý theo lô theo mô hình ELT |

Không dùng `\underline{}`. Không in đậm toàn câu.

### 2.2. Phân cấp tiêu đề

```
\chapter{Tên chương}          % Cấp 1 — dành cho chương
\section{Tên mục}             % Cấp 2 — mục lớn trong chương
\subsection{Tên tiểu mục}     % Cấp 3 — tiểu mục
\subsubsection{Tên mục nhỏ}   % Cấp 4 — chỉ dùng khi thực sự cần
```

Không dùng quá cấp 4. Tiêu đề viết súc tích, không thêm dấu chấm cuối, không in hoa toàn bộ. Hạn chế tạo mục con

### 2.3. Danh sách (`itemize` / `enumerate`)

Danh sách là **công cụ chính** của đồ án kỹ thuật dữ liệu, dùng song song
với bảng và sơ đồ — không coi là “kém học thuật”.

**Khi nên dùng `itemize`:**
- Phạm vi, giới hạn, giả định (mục 1.4.3)
- Yêu cầu chức năng / phi chức năng (Chương 2)
- Điểm thiết kế, ràng buộc vận hành (Chương 3)
- Hạn chế có chủ đích (Chương 1) và hướng mở rộng tương lai (Kết luận)

**Khi nên dùng `enumerate`:**
- Chuỗi bước pipeline (Bronze → Silver → …)
- Quy trình kiểm thử hoặc nạp bù lịch sử

**Quy tắc format:**
- Mỗi `\item` **một câu**, tối đa hai dòng; không gộp nhiều ý vào một item.
- Trước danh sách: **một câu dẫn** (vì sao liệt kê).
- Sau danh sách (tuỳ chọn): **một câu tổng kết** nếu cần nối sang mục sau.
- Nếu mỗi mục cần hơn hai câu giải thích → tách thành `\subsection` hoặc
  chuyển sang **bảng** (cột: thành phần / mô tả / ghi chú).

```latex
Phạm vi dữ liệu gồm tám nhóm sau.

\begin{itemize}
    \item Giá cổ phiếu và giá chỉ số thị trường.
    \item Danh mục niêm yết, hồ sơ doanh nghiệp, chỉ số tài chính và bảng giá.
    \item Tin tức tài chính và metadata báo cáo tài chính PDF.
\end{itemize}

Ba nhóm trên được tổ chức theo ba luồng nguồn tương ứng ở Bảng~\ref{tab:ba_luong}.
```

**Giới hạn:** tối đa **hai** khối danh sách liên tiếp trong một `\section`
(không `itemize` lồng `itemize` dài). Danh sách **không** thay bảng khi cần
so sánh theo nhiều cột (grain, khóa, chu kỳ → dùng `tabular`). Ngược lại,
**không** dùng bảng khi `itemize` hoặc hình đã đủ rõ (mục 2.4.1).

### 2.4. Bảng

**Nguyên tắc:** hạn chế tạo bảng nếu không thật sự cần. Bảng dùng để **so sánh
hoặc đối chiếu** nhiều thuộc tính cùng lúc, không dùng để ``trang trí'' hoặc
lặp lại văn xuôi.

#### 2.4.1. Khi nào dùng bảng — khi nào không

| Nên dùng bảng | Không cần bảng — dùng văn / list / hình |
|---|---|
| So sánh 2+ phương án theo cùng bộ cột (ETL vs ELT, batch vs streaming) | Liệt kê 3--8 mục cùng loại, mỗi mục một dòng mô tả |
| Ánh xạ nhiều cột: thực thể × grain × chu kỳ × khóa | Phạm vi / giới hạn dạng bullet (``ngoài phạm vi gồm:'') |
| Tra cứu kỹ thuật ở Chương 4: endpoint, mart, lịch DAG | Định nghĩa một khái niệm đã giải thích bằng 2--4 câu |
| Một bảng tổng hợp duy nhất thay cho 2 bảng trùng ý | Bảng ``đầu vào--xử lý--đầu ra'' khi đoạn văn ngay trên đã nêu đủ |
| | Bảng lặp nội dung bảng/hình ở mục khác — chỉ `\ref{}` |

**Mức độ khuyến nghị theo chương** (số bảng **tối đa** gợi ý mỗi `\section`
lớn, trừ phụ lục):

| Chương | Bảng | Ưu tiên thay thế |
|---|---|---|
| **1** — Lý thuyết | 1 bảng so sánh / `\subsection` lý thuyết | Hình khái niệm; gom bảng trùng (batch + lý do batch → một bảng) |
| **2** — Khảo sát, yêu cầu | 0--1 bảng / `\section`; còn lại list + hình | Yêu cầu chức năng/phi chức năng → `itemize` |
| **3** — Thiết kế | 1--2 bảng / `\section` (grain, partition) | Sơ đồ kiến trúc thay bảng mô tả luồng |
| **4** — Triển khai | Nhiều bảng hơn — endpoint, model, lịch | Vẫn tránh bảng 2 cột ``tên / mô tả'' lặp README |
| **5** — Đánh giá | Bảng kết quả số liệu; list hạn chế | Không bảng cho nội dung đã nêu ở Chương 1 |

**Thứ tự ưu tiên minh họa** (chọn hình thức **đầu tiên** đủ dùng):

```text
1. Văn ngắn (1--3 câu)
2. itemize / enumerate
3. figure (sơ đồ, luồng)
4. table (chỉ khi cần ≥ 2 cột thuộc tính song song)
```

**Cấm:**

- Hai bảng liên tiếp trong cùng `\subsection` nếu có thể gộp thành một.
- Bảng chỉ có 2 cột ``Khái niệm / Mô tả'' lặp lại nhiều lần trong một chương —
  gom một bảng thuật ngữ cuối chương hoặc chuyển sang list.
- Tạo bảng rồi chép nguyên nội dung bảng thành đoạn văn ngay sau.

Dùng bảng khi so sánh các thực thể theo cùng một tập thuộc tính. Không dùng
bảng cho nội dung diễn đạt được tự nhiên trong 1--2 câu hoặc trong một list
ngắn.

Bảng trong báo cáo phải đủ lớn và dễ đọc. Ưu tiên bảng chiếm toàn bộ chiều rộng
dòng văn bản, không thu nhỏ font chữ chỉ để nhét nhiều cột. Các ô phải có đường
kẻ dọc và ngang đầy đủ để người đọc phân biệt rõ từng giá trị. Hàng tiêu đề
phải được tách rõ với phần thân bảng bằng chữ in đậm và đường kẻ ngang riêng.
Nên tăng độ cao dòng và khoảng đệm trong ô bằng `\arraystretch` và `\tabcolsep`.
Caption luôn đặt dưới bảng, cách phần bảng khoảng `2pt`, sau đó mới đến `\label`.
Nếu dùng mẫu `tabularx`, bảo đảm preamble đã có `\usepackage{tabularx,array}`.

Cấu trúc chuẩn:

```latex
\begin{table}[H]
    \centering
    \renewcommand{\arraystretch}{1.25}
    \setlength{\tabcolsep}{8pt}
    \begin{tabularx}{\textwidth}{|>{\raggedright\arraybackslash}X|
                                >{\raggedright\arraybackslash}X|
                                >{\raggedright\arraybackslash}X|}
        \hline
        \textbf{Cột 1} & \textbf{Cột 2} & \textbf{Cột 3} \\
        \hline
        Giá trị        & Giá trị        & Giá trị        \\
        \hline
        Giá trị        & Giá trị        & Giá trị        \\
        \hline
    \end{tabularx}
    \vspace{2pt}
    \caption{Mô tả ngắn gọn nội dung bảng.}
    \label{tab:ten_bang}
\end{table}
```

Mỗi bảng phải có `\caption` và `\label`. Caption viết bằng tiếng Việt, đặt
dưới bảng và không được nằm quá xa phần bảng. Trước bảng trong văn bản, đặt
câu dẫn: "Bảng~\ref{tab:ten_bang} liệt kê các dataset trong lớp Silver cùng
thông tin về grain và khối lượng dữ liệu."

Sau bảng, nếu bảng chứa so sánh, phân loại hoặc quyết định quan trọng, viết
1--3 câu rút ra ý chính. Không diễn lại từng hàng; chỉ nêu điểm cần người đọc
ghi nhớ, ví dụ lý do chọn một phương án, khác biệt lớn nhất giữa các nhóm hoặc
ý nghĩa của bảng đối với mục đang viết.

### 2.5. Code và lệnh kỹ thuật

Đoạn code nhiều dòng (SQL, Python, YAML, CLI) dùng môi trường `lstlisting`:

```latex
\begin{lstlisting}[language=Python, caption={Mô tả ngắn đoạn code.},
                   label={lst:ten_label}]
def ingest_prices(config: IngestionConfig) -> None:
    df = Quote(config.ticker).history(start=start, end=end)
    _merge_monthly_parquet(df, config.output_path)
\end{lstlisting}
```

Trong văn xuôi, mô tả hành vi bằng lời thay vì trích tên hàm hay biến. Chỉ
đưa tên kỹ thuật chi tiết vào `lstlisting` hoặc bảng khi mục đó yêu cầu trình
bày cấu trúc cụ thể.

Không thêm chú thích tiếng Việt vào giữa code trong lstlisting — đặt giải
thích bên ngoài, trước hoặc sau block.

### 2.6. Hình ảnh và sơ đồ

```latex
\begin{figure}[H]
    \centering
    \includegraphics[width=0.85\textwidth]{figures/ten_hinh.png}
    \caption{Mô tả nội dung hình.}
    \label{fig:ten_hinh}
\end{figure}
```

Trước hình trong văn bản, đặt câu dẫn: "Hình~\ref{fig:ten_hinh} minh họa
kiến trúc end-to-end của hệ thống."

Sau hình, nếu hình có nhiều khối hoặc nhiều luồng, viết 1--2 câu giải thích
điểm cần rút ra từ hình. Hình ở Chương 1 phải thể hiện khái niệm chung, ví dụ
vòng đời dữ liệu, ETL/ELT, Data Warehouse--Data Lake--Lakehouse, Medallion
Architecture hoặc mô hình fact/dimension. Không dùng hình kiến trúc riêng của
stock-pipeline trong phần cơ sở lý thuyết; sơ đồ hệ thống cụ thể đặt ở Chương 3
hoặc Chương 4.

### 2.7. Dẫn chiếu chéo

Dùng `\ref{}` kết hợp `\label{}` cho mọi bảng, hình, đoạn code và mục được
nhắc đến từ nơi khác.

```latex
% Đặt nhãn tại mục được dẫn chiếu:
\section{Tầng Bronze Raw Data Lake}
\label{sec:bronze}

% Dẫn chiếu từ mục khác:
Chi tiết cơ chế merge được trình bày tại Mục~\ref{sec:bronze}.
```

Quy ước đặt tên label trong dự án này:

| Loại | Tiền tố | Ví dụ |
|---|---|---|
| Chapter | `chap:` | `\label{chap:kien_truc}` |
| Section | `sec:` | `\label{sec:bronze}` |
| Subsection | `ssec:` | `\label{ssec:price_ingestion}` |
| Table | `tab:` | `\label{tab:silver_datasets}` |
| Figure | `fig:` | `\label{fig:medallion_arch}` |
| Listing | `lst:` | `\label{lst:upsert_sql}` |

### 2.8. Ký hiệu và dấu câu trong LaTeX

- Dấu gạch ngang dài (em dash): dùng `---` không dùng `-` đơn.
- Dấu gạch ngang ngắn (en dash, dùng trong khoảng): dùng `--`.
- Dấu ngoặc kép tiếng Việt: dùng ``...'' (hai backtick mở, hai nháy đơn
  đóng) hoặc gói `csquotes` với lệnh `\enquote{}`.
- Ký hiệu phần trăm: `50\%` không phải `50%`.
- Dấu chấm trong số nguyên lớn (phân cách hàng nghìn): dùng dấu chấm theo
  quy ước tiếng Việt: `62.339` không phải `62,339`.

---

## 3. Cấu trúc và mạch lạc

### 3.1. Trình tự trong một mục

Khung **Mở — Giải thích — Minh họa — Nhận xét — Chuyển ý** (không ghi các
tiêu đề này ra LaTeX):

1. **Mở:** 1--3 câu nêu mục này giải quyết gì và vì sao nó xuất hiện trong chương.
2. **Giải thích nền:** với mục lý thuyết, nêu định nghĩa chung, đặc điểm, vai trò
   và giới hạn; với mục thiết kế/triển khai, nêu bài toán hoặc tiêu chí kỹ thuật.
3. **Minh họa:** dùng bảng, hình hoặc list khi có từ ba thực thể trở lên, khi cần
   so sánh, hoặc khi luồng xử lý khó đọc nếu chỉ viết bằng văn.
4. **Nhận xét:** 1--3 câu rút ra ý chính từ bảng/hình/list; không đọc lại từng
   hàng hoặc từng nhãn trong hình.
5. **Chuyển ý:** 0--1 câu nêu output, dẫn chiếu `\ref{}` hoặc nối sang mục tiếp theo.

Không mở bằng định nghĩa từ điển khô cứng rồi dừng ở đó. Không viết đoạn kết
dài sau bảng/list, nhưng cũng không bỏ trống phần nhận xét khi bảng/hình chứa
ý quan trọng.

### 3.2. Liên kết giữa các mục

Khi một mục đề cập đến thành phần được giải thích ở mục khác, dẫn chiếu rõ
bằng `\ref{}`: "Chi tiết được trình bày tại Mục~\ref{sec:bronze}" hoặc
"Xem Mục~\ref{sec:upsert}". Không bọc dẫn chiếu trong ngoặc đơn. Không để
người đọc tự suy diễn.

Khi kết thúc một mục mô tả một lớp trong pipeline, câu cuối nên nêu output
của lớp và đầu vào mà lớp tiếp theo nhận: "Kết quả của bước này là tập
dữ liệu sạch ở định dạng Parquet; đây cũng là nguồn duy nhất cho bước nạp
vào kho dữ liệu quan hệ ở Mục~\ref{sec:loader}."

### 3.3. Tránh lặp thông tin

Mỗi thông tin cụ thể (ví dụ: "50 mã chứng khoán", "PostgreSQL 15", "5 năm
lịch sử") chỉ được trình bày đầy đủ một lần tại mục phù hợp nhất. Ở các mục
khác, nếu cần nhắc lại, dùng dạng ngắn gọn hoặc `\ref{}`.

### 3.4. Quy tắc viết phần phân tích và thiết kế

Phần thiết kế, đặc biệt là Chương 3, trả lời câu hỏi: **vì sao hệ thống được
thiết kế như vậy**. Không viết Chương 3 như hướng dẫn chạy chương trình, không
liệt kê module, lệnh, file hoặc tham số vận hành như Chương 4.

Một tiểu mục thiết kế nên có đủ các lớp ý sau:

1. **Bài toán hoặc ràng buộc:** dữ liệu có đặc điểm gì, yêu cầu nào buộc phải xử lý.
2. **Quyết định thiết kế:** hệ thống chọn cách tổ chức hoặc xử lý nào.
3. **Lý do lựa chọn:** quyết định đó giải quyết ràng buộc nào, có đánh đổi gì.
4. **Hợp đồng dữ liệu:** đầu vào, đầu ra, grain, khóa nghiệp vụ hoặc ranh giới trách nhiệm.
5. **Kiểm soát rủi ro:** cách hạn chế trùng lặp, thiếu dữ liệu, sai schema, lỗi chạy lại hoặc mất truy vết.
6. **Liên hệ triển khai:** chỉ nêu ngắn rằng chi tiết hiện thực được trình bày ở Chương 4 nếu cần.

Khi viết thiết kế dữ liệu, ưu tiên các khái niệm: grain, khóa nghiệp vụ,
partition, watermark, cập nhật gia tăng, khử trùng lặp, upsert, audit và
lineage. Khi viết thiết kế kiến trúc, ưu tiên ranh giới giữa các tầng, luồng dữ
liệu, quyền đọc/ghi và trách nhiệm của từng lớp. Khi viết thiết kế API hoặc
dashboard, ưu tiên hợp đồng dữ liệu, miền truy vấn, trạng thái chỉ đọc và nguồn
dữ liệu phân tích; không biến mục này thành danh sách endpoint chi tiết.

Thiết kế DAG ở Chương 3 chỉ mô tả chế độ vận hành, quan hệ phụ thuộc, thứ tự
luồng và logic backfill/incremental ở mức khái niệm. Lịch chạy cụ thể, tên task,
lệnh CLI, biến môi trường và cấu hình runtime thuộc Chương 4 hoặc phụ lục.

Nếu một quyết định thiết kế quan trọng có phương án thay thế, có thể trình bày
ngắn theo cấu trúc: **phương án được chọn -- lý do chọn -- phương án không chọn
-- hạn chế còn lại**. Không cần so sánh phương án cho mọi chi tiết nhỏ.

Với mục lục hiện tại của stock-pipeline, Chương 3 nên bám các trọng tâm thiết
kế sau:

- **Định hướng kiến trúc dữ liệu:** giải thích vì sao chọn Medallion, Batch ELT
  và hướng lakehouse-style; chưa mô tả module triển khai.
- **Kiến trúc tổng thể:** trình bày ranh giới giữa nguồn, Bronze, Silver,
  PostgreSQL Silver, dbt Gold, API và dashboard; sơ đồ phải cho thấy luồng dữ
  liệu, không phải cây thư mục.
- **Tầng Bronze:** tập trung vào vai trò lưu gần nguồn, nhóm nguồn dữ liệu và
  chiến lược phân vùng; không đưa chi tiết notebook hoặc lệnh crawl.
- **Tầng Silver:** nêu chuẩn hóa schema, kiểu dữ liệu, grain, khóa nghiệp vụ,
  khử trùng lặp, kiểm soát chất lượng, watermark và cập nhật gia tăng.
- **Warehouse và Gold:** nêu cơ chế nạp Silver vào kho quan hệ, upsert, audit,
  mô hình dbt theo staging, intermediate, fact, dimension và mart, cùng lineage
  đến API.
- **Điều phối:** nêu chế độ backfill, incremental, quan hệ phụ thuộc giữa các
  luồng và nguyên tắc làm mới Gold; lịch cụ thể và task code để Chương 4.
- **Lớp phục vụ dữ liệu:** nhấn mạnh backend chỉ đọc từ Gold, API theo miền dữ
  liệu, dashboard phân tích tài chính và phục vụ PDF; không mô tả frontend như
  hướng dẫn sử dụng giao diện.

---

## 4. Bám sát codebase

### 4.1. Quy tắc bắt buộc

Trước khi viết, đọc codebase để đảm bảo nội dung đúng sự thật kỹ thuật.
Tuy nhiên, **báo cáo không phải tài liệu mã nguồn**: người đọc là hội đồng
và giảng viên, không phải lập trình viên bảo trì repo. Vì vậy:

- Đúng sự thật lấy từ code, nhưng **diễn đạt bằng lời** trong văn xuôi.
- Không trích đường dẫn thư mục, tên file, tên hàm, endpoint hay schema trừ
  khi mục đó bắt buộc trình bày chi tiết triển khai (ví dụ mục thiết kế API
  có thể dùng bảng endpoint).
- Không liệt kê dài tên mart, tên bảng, tên DAG trong một đoạn; gom theo
  chức năng: ``nhóm mart phục vụ giá và chỉ báo'', ``nhóm mart phục vụ tin
  tức''.

Các file tham chiếu chính theo từng chủ đề:

| Chủ đề | File tham chiếu |
|---|---|
| Ingestion Bronze | `ingestion/structure_data/config.py`, `pipeline.py`, `price_ingestor.py`, `stock_info_ingestor.py` |
| Silver transform | `pipeline/silver/cli.py`, các transformer tương ứng |
| Warehouse loader | `warehouse/loader/silver_loader.py`, `warehouse/loader/cli.py` |
| Gold — dbt models | `transform/dbt/models/**/*.sql`, `dbt_project.yml` |
| API backend | `backend/routers/*.py`, `backend/schemas/*.py` |
| Frontend | `frontend/src/api/*.ts`, `frontend/src/pages/*.tsx` |
| Luồng tổng thể | `Docs/Structure_data_flow.md`, `Docs/News_data_flow.md`, `Docs/BCTC_data_flow.md`, `README.md` |

### 4.2. Cách trình bày số liệu và tham số

Khi nêu giá trị mặc định, diễn đạt bằng lời: "cấu hình mặc định giới hạn
50 yêu cầu mỗi phút; notebook backfill có thể tăng lên 60 yêu cầu mỗi phút
khi chạy thử nghiệm." Chỉ đưa tên class config vào bảng hoặc phụ lục nếu
cần.

Khi nêu số dòng dữ liệu, ghi rõ thời điểm và diễn đạt theo nghiệp vụ:
"tính đến ngày 2026-06-01, dữ liệu giá trong kho Silver chứa khoảng 62.339
bản ghi, với ngày giao dịch mới nhất là 2026-05-18." Không viết số liệu
không có mốc thời gian.

### 4.3. Mô tả luồng dữ liệu

Mô tả luồng theo hướng dữ liệu đi, không theo hướng code được gọi.

**Đúng:** "Sau khi lấy dữ liệu OHLCV từ nguồn vnstock, hệ thống gom dữ liệu
theo tháng giao dịch, hợp nhất với dữ liệu tháng đã có nếu tồn tại, rồi ghi
đè bản lưu tháng đó."

**Sai:** "Hàm merge monthly parquet được gọi bởi ingest prices để…" — mô tả
call stack và trích danh từ mã nguồn, không phải luồng dữ liệu.

**Sai:** "Hệ thống ghi audit tại data-lake/silver/price/\_runs.jsonl" — trích
đường dẫn thay vì giải thích: "Hệ thống có cơ chế ghi nhật ký vận hành sau
mỗi lần biến đổi ở tầng Silver."

### 4.4. Từ điển thay thế — tên kỹ thuật → tiếng Việt báo cáo

Dùng bảng này khi viết hoặc sửa Chương 2--3. Cột trái **không** đưa vào văn xuôi.

| Trong code / README | Viết trong báo cáo |
|---|---|
| `listing` | danh mục niêm yết |
| `price` | giá cổ phiếu |
| `index_price` | giá chỉ số |
| `company` | hồ sơ doanh nghiệp / hồ sơ công ty |
| `financial_ratio` | chỉ số tài chính |
| `price_board` | bảng giá |
| `news` | tin tức |
| `bctc_pdf_meta` | metadata báo cáo tài chính PDF |
| `fact_price_daily` | bảng fact giá theo ngày |
| `mart_stock_daily` | mart tổng hợp giá và chỉ báo theo mã |
| `mart_market_overview` | mart tổng quan thị trường |
| `int_price_indicator` | bảng chỉ báo kỹ thuật trung gian |
| `dim_company`, `dim_security` | dimension doanh nghiệp / chứng khoán |
| `structured_daily` | DAG hàng ngày luồng có cấu trúc |
| `structured_monthly` | DAG hàng tháng luồng có cấu trúc |
| `news_daily` | DAG hàng ngày luồng tin tức |
| `bctc_quarterly` | DAG quý luồng báo cáo tài chính |
| `gold_full_refresh` | tác vụ làm mới toàn bộ tầng Gold cuối ngày |
| `use_listing_as_universe` | universe lấy từ danh mục niêm yết |
| `listing_security_type_filter=stock` | lọc chỉ cổ phiếu trên HOSE và HNX |
| `_watermark.json` | file watermark tập trung |
| `_runs.jsonl` | nhật ký vận hành sau mỗi lần biến đổi |
| `silver.load_audit` | bảng audit nạp dữ liệu |
| `ON CONFLICT DO UPDATE` | cập nhật khi xung đột khóa |
| `fetched_at`, `snapshot_at` | thời điểm thu thập / thời điểm snapshot |
| `article_id`, `doc_id` | mã bài viết / mã tài liệu |
| `watchlist50` | 50 mã trong danh sách theo dõi |

### 4.5. Phụ lục vs văn xuôi — khi nào được trích mã

| Nội dung | Văn xuôi | Phụ lục / bảng / lstlisting |
|---|---|---|
| Luồng Bronze → Silver → Gold | Mô tả vai trò từng tầng | Sơ đồ + bảng ánh xạ tùy chọn |
| Cây thư mục `data-lake/` | Không | Có — Phụ lục "Cấu trúc triển khai" |
| Danh sách endpoint API | Mô tả chức năng nhóm API | Bảng endpoint nếu mục thiết kế API |
| Tham số `IngestionConfig` | Nêu giá trị bằng lời | Bảng tham số nếu cần minh chứng |
| Grain và khóa upsert | Bảng thiết kế (cột tiếng Việt) | Cột kỹ thuật chỉ ở phụ lục triển khai |

### 4.6. Quy tắc theo chương

| Chương | Mức độ chi tiết mã | Cách trình bày ưu tiên |
|---|---|---|
| **Chương 1** — Tổng quan, lý thuyết | Thấp | Định nghĩa chung; 1 bảng so sánh / tiểu mục lý thuyết; hình khái niệm; liên hệ dự án 1--2 câu |
| **Chương 2** — Khảo sát dữ liệu và yêu cầu hệ thống | Thấp–trung bình | Đặc điểm nguồn, yêu cầu chức năng/phi chức năng; ưu tiên hình + `itemize`; bảng chỉ dùng khi so sánh nguồn hoặc yêu cầu |
| **Chương 3** — Phân tích và thiết kế kiến trúc hệ thống | Trung bình | Quyết định thiết kế + lý do; bảng grain/khóa/partition; sơ đồ kiến trúc; tránh lệnh, path, lịch chạy chi tiết |
| **Chương 4** — Triển khai hệ thống | Cao ở mục chuyên | Môi trường, cấu trúc mã nguồn, pipeline, dbt, Airflow, API, dashboard; có thể dùng `lstlisting` khi cần minh chứng |
| **Chương 5** — Kiểm thử và đánh giá hệ thống | Trung bình | Phương pháp kiểm thử, kết quả xử lý, chất lượng dữ liệu, vận hành, API/dashboard; không lặp lại yêu cầu ở Chương 2 |

Trong mọi chương: **không** tên dataset/path trong văn xuôi; **có thể** tên
kỹ thuật trong bảng triển khai (Chương 4) nếu cột có tiêu đề rõ (ví dụ cột
``Tên model'' trong phụ lục).

### 4.7. Sự thật kỹ thuật cần nhớ (stock-pipeline)

Agent phải khớp các điểm sau khi viết Chương 2--3 (đọc lại code nếu nghi ngờ):

- **8 tập dữ liệu Silver:** giá cổ phiếu, giá chỉ số, niêm yết, hồ sơ công ty, chỉ số tài chính, bảng giá, tin tức, metadata BCTC PDF.
- **3 luồng nguồn:** có cấu trúc (vnstock), tin tức (RSS/HTML), BCTC (HNX PDF).
- **Universe mặc định structured:** danh mục niêm yết Bronze, lọc HOSE + HNX + **chỉ cổ phiếu** (~700 mã); không mô tả ~1052 mã mọi loại CK.
- **DAG hàng ngày structured:** giá, chỉ số, bảng giá; mặc định 50 mã watchlist.
- **DAG hàng tháng structured:** niêm yết, hồ sơ công ty, chỉ số tài chính; universe cổ phiếu HOSE/HNX.
- **Bronze không nạp PostgreSQL;** Silver Parquet → loader → schema silver → dbt gold → FastAPI → React.
- **BCTC:** lưu metadata + PDF; chưa OCR/trích bảng số.

---

## 5. Quy trình viết một mục

Khi nhận yêu cầu viết một mục cụ thể (ví dụ "viết mục 2.3 — Tầng Bronze"):

**Bước 1 — Xác định phạm vi:** Đọc tiêu đề và các tiêu đề con của mục trong
outline báo cáo. Nếu chưa có outline, hỏi người dùng mục cần bao gồm những
nội dung gì.

**Bước 2 — Đọc source:** Đọc các file codebase tương ứng theo bảng mục 4.1.
Ghi chú: tên hàm entry point, tên cột output, giá trị mặc định của tham số
quan trọng, thứ tự xử lý, cơ chế xử lý lỗi nếu có.

**Bước 3 — Lập dàn ý và chọn hình thức:** Với mỗi ý, quyết định trước theo thứ
tự mục 2.4.1: văn ngắn → list → hình → bảng (chỉ khi cần so sánh nhiều cột).
Khung: mở mục → giải thích nền → minh họa bằng hình/list/bảng nếu cần → nhận
xét → chuyển ý. Không bỏ qua bước này dù mục ngắn.

**Bước 4 — Viết nháp LaTeX:** Viết theo dàn ý. Nếu thiếu thông tin, đặt
placeholder \texttt{[CẦN XÁC NHẬN: …]} và tiếp tục — không tự bịa giá trị
hay tên hàm.

**Bước 5 — Rà soát văn phong:** Đọc lại từng `\section`: (a) cắt câu trên
25 từ; (b) thay cụm mục 1.1; (c) gom liệt kê vào bảng/list nếu còn nằm trong
văn; (d) bảo đảm bảng/hình có nhận xét sau nhưng không chép lại nội dung từng
hàng. Chạy checklist mục 0.4.

**Bước 6 — Rà soát thiết kế nếu viết Chương 3:** Mỗi mục phải trả lời được
"vì sao chọn thiết kế này". Nếu đoạn văn chủ yếu là lệnh chạy, lịch DAG,
endpoint, tên file hoặc cấu hình runtime, chuyển nội dung đó sang Chương 4.

**Bước 7 — Rà soát kỹ thuật:** Không còn tên dataset dạng `price`, `listing`
trong văn xuôi; bảng dùng cột tiếng Việt; `\ref{}` trỏ label tồn tại.

**Bước 8 — Khi user yêu cầu "sửa cho phù hợp báo cáo":** Áp dụng mục 0.5;
xuất file `Docs/latex/ch{N}_*_revised.tex` hoặc block LaTeX hoàn chỉnh.

---

## 6. Các lỗi cần tránh

| Lỗi thường gặp | Cách sửa |
|---|---|
| Định nghĩa kiểu từ điển một câu rồi bỏ qua giải thích | Bổ sung vai trò, đặc điểm, so sánh/giới hạn và liên hệ ngắn |
| Dùng `\textbf{}`, `\textit{}` trong văn xuôi | Bỏ định dạng; viết câu thuần |
| Bọc mọi tên kỹ thuật bằng `\texttt{}` | Chỉ dùng trong bảng/lstlisting; văn xuôi dùng từ Việt hoặc tên riêng không format |
| Trích đường dẫn, endpoint, tên file trong văn xuôi | Giải thích chức năng bằng lời |
| Chen tiếng Anh trong ngoặc đơn | Dùng thuật ngữ Việt; chỉ giữ tên riêng công nghệ khi bắt buộc |
| Mở ngoặc giải thích phụ liên tục | Tách thành câu riêng |
| Dùng `%` thay vì `\%` | Thay toàn bộ trước khi compile |
| Dùng `"..."` thay vì \`\`...'' cho ngoặc kép | Dùng backtick đôi mở, nháy đơn đôi đóng |
| Bảng/hình không có `\caption` và `\label` | Bắt buộc có cả hai; đặt câu dẫn trước bảng/hình trong văn bản |
| Dẫn chiếu bằng "Mục X.Y" hardcode | Dùng `Mục~\ref{sec:...}` để dẫn chiếu tự động |
| Mô tả call stack thay vì luồng dữ liệu | Viết theo hướng dữ liệu đi, không theo hướng hàm được gọi |
| Số liệu không có mốc thời gian | Thêm "tính đến ngày YYYY-MM-DD" trước số liệu snapshot |
| Dùng "v.v.", "và nhiều hơn nữa" | Liệt kê đầy đủ hoặc nêu tiêu chí chọn lọc rõ ràng |
| Lặp lại cùng thông tin ở nhiều mục | Viết đầy đủ một lần, dẫn chiếu `\ref{}` ở các mục còn lại |
| Viết như README kỹ thuật nội bộ | Cấu trúc báo cáo: văn ngắn + bảng/list + hình; không path/hàm trong thân văn |
| Văn xuôi dài như luận văn toán | Rút gọn; chuyển liệt kê sang bảng hoặc `itemize` |
| Cụm ``đóng vai trò'', ``tạo điều kiện'', ``mang ý nghĩa'' | Viết trực tiếp: ``dùng để'', ``phục vụ'', ``gồm'' (xem mục 1.1) |
| Mở đoạn ``Trong thực tế vận hành'' / ``Song song với'' | Vào thẳng nội dung mục |
| Bảng/hình xuất hiện nhưng không có đoạn giải thích ý nghĩa | Thêm 1--3 câu nhận xét sau bảng/hình; không diễn lại từng dòng |
| Tóm tắt lại bảng bằng 1 đoạn văn dài | Rút còn nhận xét chính; chỉ `\ref{}` nếu cần |
| Tránh mọi `itemize` vì “không học thuật” | Dùng list có kiểm soát (mục 2.3); học thuật ≠ văn dài |
| Một `\section` toàn đoạn văn, không hình/list | Chương 2--4: thêm hình hoặc list khi mục có từ 3 thực thể trở lên |
| Tạo bảng cho mọi liệt kê 3+ mục | Dùng `itemize`; bảng chỉ khi so sánh nhiều cột (mục 2.4.1) |
| Hai bảng trùng ý trong cùng `\subsection` | Gộp một bảng hoặc bỏ bảng thứ hai, dùng `\ref{}` |
| Chương 2 toàn bảng, ít hình | Chuyển yêu cầu/giới hạn sang list; giữ 1 bảng grain hoặc so sánh nguồn nếu cần |
| `\label` đặt trước `\caption` trong bảng | Luôn `\caption` rồi `\label` |
| Liệt kê tên file module trong đoạn kiến trúc | Phân lớp theo *vai trò*: cấu hình, thu thập giá, điều phối… |
| Đoạn mô tả nội dung hình thay vì câu dẫn | Một câu dẫn + `\begin{figure}`; không "Hình X: sáu khối…" |
| Chương 1 dùng hình kiến trúc riêng của dự án cho phần lý thuyết | Đổi thành hình định nghĩa/khái niệm chung; để sơ đồ dự án sang Chương 3 |
| Mục cơ sở lý thuyết chỉ có định nghĩa ngắn rồi chuyển ngay sang dự án | Viết định nghĩa chung, đặc điểm, so sánh/ưu nhược, ví dụ khái quát, rồi mới liên hệ ngắn |
| Chương 3 chỉ liệt kê tầng, bảng hoặc công nghệ mà không giải thích lý do | Thêm bài toán/ràng buộc, quyết định thiết kế, lý do chọn và hệ quả thiết kế |
| Chương 3 đưa lệnh chạy, lịch DAG, biến môi trường hoặc đường dẫn triển khai | Chuyển sang Chương 4 hoặc phụ lục; Chương 3 chỉ giữ chế độ vận hành và quan hệ phụ thuộc ở mức thiết kế |
| Mục thiết kế API biến thành danh sách endpoint | Viết theo miền dữ liệu, hợp đồng đọc từ Gold, bộ lọc chính và nguyên tắc chỉ đọc; endpoint chi tiết để Chương 4 |
| Mục thiết kế dashboard mô tả giao diện như hướng dẫn sử dụng | Viết theo nhu cầu phân tích, nguồn dữ liệu Gold, trạng thái lọc và luồng tra cứu của người dùng |
| Dùng `structured` thay vì tiếng Việt | *luồng có cấu trúc* |
| Ghi universe 1052 mã HOSE/HNX không lọc stock | ~700 mã cổ phiếu sau lọc stock (đối chiếu `listing_security_type_filter`) |
| Tham chiếu `Mục~\ref{sec:airflow}` khi label chưa có | `Chương~2`, `Chương~4` hoặc tạo `\label` trước |
| Agent chỉ phân tích không xuất LaTeX đã sửa | User gửi LaTeX → trả bản đã sửa đầy đủ |

---

## 7. Ví dụ minh họa

### Ví dụ SAI — văn quá học thuật (luận văn thuần văn)

```latex
Trong bối cảnh thị trường chứng khoán Việt Nam đang trải qua giai đoạn tăng
trưởng mạnh mẽ, nhu cầu khai thác dữ liệu đa nguồn ngày càng trở thành yêu
cầu then chốt. Dữ liệu có cấu trúc đóng vai trò nền tảng, trong khi dữ liệu
phi cấu trúc và bán cấu trúc tạo điều kiện bổ sung góc nhìn phân tích. Việc
xây dựng pipeline vì vậy mang ý nghĩa quan trọng trong việc hình thành một
hệ sinh thái dữ liệu thống nhất...
```

Lỗi: câu dài, từ hoa, lặp ý, không có bảng/sơ đồ, không nêu thiết kế cụ thể.

---

### Ví dụ SAI — viết theo kiểu tài liệu kỹ thuật nội bộ

```latex
\textbf{Bronze Layer} là nơi lưu trữ raw data. \textbf{IngestionConfig} có
các tham số như tickers, years\_back, rate\_limit\_rpm. Hàm ingest\_prices()
gọi Quote.history() để lấy OHLCV. Sau đó \_merge\_monthly\_parquet() được
gọi để merge. Output là file .parquet.
```

Lỗi: in đậm tùy tiện, không dùng `\texttt{}` cho tên kỹ thuật, mô tả call
stack, thiếu bối cảnh, không nêu lý do thiết kế, không có số liệu cụ thể.

---

### Ví dụ ĐÚNG — phong cách đồ án kỹ thuật dữ liệu (văn ngắn + bảng)

```latex
Lớp Bronze lưu dữ liệu gần nguồn nhất, trước bước chuẩn hóa ở Silver.
Bảng~\ref{tab:bronze_partition} tóm tắt chiến lược phân vùng theo từng
luồng nguồn.

% ... bảng partition ba luồng ...

Giá cổ phiếu thu thập qua vnstock; mỗi lần chạy hợp nhất theo tháng giao
dịch và giữ bản ghi mới nhất theo mã và ngày. Tính đến ngày 2026-06-01,
dữ liệu giá tích lũy cho 50 mã với khoảng 62.339 bản ghi ở Silver
(xem Mục~\ref{sec:silver_price}).
```

### Ví dụ ĐÚNG — phạm vi và giới hạn (văn + itemize)

```latex
Đề tài tập trung xây dựng pipeline phân tích batch, không mở rộng sang
các hướng sau:

\begin{itemize}
    \item Trích xuất bảng số liệu từ PDF báo cáo tài chính bằng OCR.
    \item Xử lý dữ liệu intraday theo thời gian thực.
    \item Xác thực người dùng và triển khai production trên cloud.
\end{itemize}

Các giới hạn trên là có chủ đích, phù hợp phạm vi đồ án và trạng thái
triển khai hiện tại của hệ thống.
```

---

### Ví dụ bảng đúng chuẩn

```latex
Bảng~\ref{tab:silver_datasets} tổng hợp tám nhóm dữ liệu ở tầng Silver cùng
đơn vị chi tiết và chu kỳ cập nhật.

\begin{table}[ht]
    \centering
    \renewcommand{\arraystretch}{1.25}
    \setlength{\tabcolsep}{8pt}
    \begin{tabularx}{\textwidth}{|>{\raggedright\arraybackslash}X|
                                >{\raggedright\arraybackslash}X|
                                >{\raggedright\arraybackslash}X|
                                >{\raggedright\arraybackslash}X|}
        \hline
        \textbf{Nhóm dữ liệu} & \textbf{Đơn vị chi tiết} & \textbf{Chu kỳ} & \textbf{Khóa cập nhật} \\
        \hline\hline
        Giá cổ phiếu & Mã + ngày giao dịch & Hàng ngày T2--T6 & Mã và ngày \\
        \hline
        Giá chỉ số & Mã chỉ số + ngày & Hàng ngày T2--T6 & Mã chỉ số và ngày \\
        \hline
        Hồ sơ công ty & Mã cổ phiếu & Định kỳ & Mã cổ phiếu \\
        \hline
        Chỉ số tài chính & Mã + kỳ báo cáo & Định kỳ & Mã, chỉ tiêu và kỳ \\
        \hline
    \end{tabularx}
    \vspace{2pt}
    \caption{Đặc trưng thiết kế các nhóm dữ liệu ở tầng Silver.}
    \label{tab:silver_datasets}
\end{table}
```

Trong bảng, chỉ dùng tiếng Việt cho cột mô tả; tránh lặp tên schema hay tên
cột kỹ thuật nếu mục đó nhằm giải thích thiết kế, không phải liệt kê mã
nguồn.

---

## 8. Tham chiếu nhanh theo mục lục báo cáo

Bảng dưới ánh xạ mục lục chính thức hiện tại sang nội dung cần trình bày và
file source cần đọc trước khi viết. Khi mục lục thay đổi, cập nhật bảng này
trước khi viết hoặc sửa chương mới.

| Chương / Mục | Nội dung chính cần trình bày | File source cần đọc |
|---|---|---|
| **Chương 1** — Tổng quan đề tài và cơ sở lý thuyết | | |
| 1.1 Bối cảnh và lý do chọn đề tài | Bối cảnh dữ liệu chứng khoán đa nguồn, nhu cầu tập trung hóa dữ liệu phục vụ phân tích | `README.md`, `Docs/*.md` |
| 1.2 Phát biểu bài toán | Đầu vào, xử lý theo lô, đầu ra API/dashboard; không đi vào thiết kế chi tiết | `README.md`, `Docs/Structure_data_flow.md`, `Docs/News_data_flow.md`, `Docs/BCTC_data_flow.md` |
| 1.3 Mục tiêu của đề tài | Mục tiêu kỹ thuật và mục tiêu nghiệp vụ phân tích ở mức tổng quan | `README.md`, codebase chính để kiểm chứng phạm vi |
| 1.4 Phạm vi và giới hạn của đề tài | Phạm vi dữ liệu, phạm vi chức năng, giới hạn có chủ đích | `README.md`, `Docs/*.md` |
| 1.5 Cơ sở lý thuyết | Định nghĩa chung: Data Pipeline, Batch, ETL/ELT, Data Lake, Data Warehouse, Lakehouse, Medallion, fact/dimension/mart, orchestration, serving layer | Tài liệu lý thuyết bên ngoài + đối chiếu nhẹ với codebase |
| **Chương 2** — Khảo sát dữ liệu và phân tích yêu cầu hệ thống | | |
| 2.1 Khảo sát nguồn dữ liệu | Ba nhóm nguồn: có cấu trúc, phi cấu trúc, bán cấu trúc; nêu đặc điểm từng nhóm dữ liệu | `Docs/Structure_data_flow.md`, `Docs/News_data_flow.md`, `Docs/BCTC_data_flow.md`, `ingestion/` |
| 2.2 Đặc điểm dữ liệu chứng khoán đa nguồn | Thời gian giao dịch, snapshot, chuỗi thời gian, trùng lặp, thiếu dữ liệu, sai khác giữa nguồn | `Docs/*.md`, `pipeline/silver/`, `tests/` |
| 2.3 Yêu cầu chức năng | Thu thập, lưu gần nguồn, chuẩn hóa, nạp kho, xây lớp phục vụ API/dashboard, điều phối theo lịch | `README.md`, `Docs/*.md`, các thư mục `ingestion/`, `pipeline/silver/`, `warehouse/`, `transform/dbt/`, `backend/`, `frontend/` |
| 2.4 Yêu cầu phi chức năng | Idempotency, cập nhật gia tăng, kiểm soát chất lượng, truy vết/audit, mở rộng nguồn và mô hình | `Docs/*.md`, `warehouse/loader/`, `pipeline/silver/runs_log.py`, `transform/dbt/models/**/*.yml`, `tests/` |
| **Chương 3** — Phân tích và thiết kế kiến trúc hệ thống | | |
| 3.1 Định hướng kiến trúc dữ liệu của hệ thống | Lý do chọn lakehouse-style, Medallion, Batch ELT ở mức quyết định thiết kế | `README.md`, `Docs/*.md` |
| 3.2 Kiến trúc tổng thể hệ thống | Sơ đồ tổng thể pipeline, các tầng xử lý, luồng từ nguồn đến dashboard | `README.md`, `Docs/*.md`, toàn bộ thư mục chính |
| 3.3 Thiết kế tầng Bronze | Vai trò Bronze, lưu trữ dữ liệu có cấu trúc, tin tức, metadata/PDF, chiến lược phân vùng | `Docs/Structure_data_flow.md`, `Docs/News_data_flow.md`, `Docs/BCTC_data_flow.md`, `ingestion/` |
| 3.4 Thiết kế tầng Silver | Chuẩn hóa schema/kiểu dữ liệu, grain, khóa nghiệp vụ, khử trùng lặp, chất lượng dữ liệu, watermark | `pipeline/silver/`, `Docs/*.md`, `tests/silver/` |
| 3.5 Thiết kế lớp Warehouse và Gold | Nạp Silver vào PostgreSQL, upsert/audit, dbt staging/intermediate/fact/dimension/mart, lineage tới API | `warehouse/`, `transform/dbt/`, `Docs/dbt_outputs_and_lineage.md` |
| 3.6 Thiết kế điều phối pipeline | Chế độ backfill/incremental, thiết kế DAG theo luồng, làm mới Gold ở mức phụ thuộc và thứ tự xử lý | `docker/airflow/dags/`, `Docs/*.md` |
| 3.7 Thiết kế lớp phục vụ dữ liệu | Backend chỉ đọc từ Gold, API theo miền dữ liệu, dashboard phân tích, phục vụ PDF | `backend/`, `frontend/`, `transform/dbt/models/marts/` |
| **Chương 4** — Triển khai hệ thống | | |
| 4.1 Môi trường và công nghệ sử dụng | Ngôn ngữ, thư viện, lưu trữ, cơ sở dữ liệu, công cụ chuyển đổi/điều phối/phục vụ, cấu trúc mã nguồn | `README.md`, `requirements.txt`, `backend/requirements.txt`, `frontend/package.json`, `docker-compose.yml`, `transform/dbt/dbt_project.yml` |
| 4.2 Triển khai pipeline dữ liệu có cấu trúc | Thu thập từ vnstock, lưu Bronze, biến đổi Silver, nạp warehouse và mart phân tích | `ingestion/structure_data/`, `pipeline/silver/structure_transformer.py`, `pipeline/silver/price_transformer.py`, `pipeline/silver/financial_ratio_transformer.py`, `pipeline/silver/price_board_transformer.py`, `warehouse/`, `transform/dbt/` |
| 4.3 Triển khai pipeline dữ liệu tin tức | RSS/HTML, chuẩn hóa, khử trùng lặp, gán mã cổ phiếu, fact/mart tin tức | `ingestion/unstructured_data/`, `pipeline/silver/news_transformer.py`, `transform/dbt/models/**/*news*` |
| 4.4 Triển khai pipeline metadata và PDF báo cáo tài chính | Crawl HNX, phân loại tài liệu, tải PDF, chuẩn hóa metadata, mart tài liệu | `ingestion/semi_structure_data/`, `pipeline/silver/bctc_pdf_meta_transformer.py`, `transform/dbt/models/**/*bctc*` |
| 4.5 Triển khai mô hình dbt Gold | Cấu hình dbt, staging, intermediate, fact, dimension, mart, incremental, kiểm thử dbt | `transform/dbt/dbt_project.yml`, `transform/dbt/models/`, `transform/dbt/tests/` |
| 4.6 Triển khai điều phối bằng Airflow | Cấu hình môi trường Airflow, các DAG chính, truyền partition, retry, theo dõi trạng thái chạy | `docker/airflow/dags/`, `docker/airflow/dags/common/` |
| 4.7 Triển khai FastAPI backend | Cấu trúc backend, nhóm endpoint, phân trang, lọc dữ liệu, xử lý lỗi; nhấn mạnh chỉ đọc từ Gold | `backend/main.py`, `backend/routers/`, `backend/schemas/`, `backend/database.py` |
| 4.8 Triển khai React dashboard | Cấu trúc frontend, tổng quan thị trường, chi tiết mã, tin tức, tài liệu báo cáo tài chính | `frontend/src/pages/`, `frontend/src/components/`, `frontend/src/api/`, `frontend/src/hooks/` |
| **Chương 5** — Kiểm thử và đánh giá hệ thống | | |
| 5.1 Phương pháp kiểm thử | Kiểm thử từng tầng pipeline, dữ liệu sau biến đổi, dbt models, API và dashboard | `tests/`, `transform/dbt/models/**/*.yml`, `transform/dbt/tests/` |
| 5.2 Đánh giá kết quả thu thập và xử lý dữ liệu | Kết quả dữ liệu có cấu trúc, tin tức, metadata và PDF BCTC | Kết quả chạy pipeline, `Docs/*.md`, warehouse/Gold outputs nếu có |
| 5.3 Đánh giá chất lượng dữ liệu | Tính đầy đủ, nhất quán, trùng lặp, khóa nghiệp vụ, truy vết/audit | `tests/`, `pipeline/silver/`, `warehouse/loader/`, dbt tests |
| 5.4 Đánh giá khả năng vận hành pipeline | Backfill, incremental, chạy lại an toàn, điều phối theo lịch | `docker/airflow/dags/`, `pipeline/silver/`, `warehouse/loader/`, `Docs/*.md` |
| 5.5 Đánh giá API và dashboard | Tra cứu đa miền, phân tích theo mã cổ phiếu, phục vụ tin tức và tài liệu BCTC | `backend/`, `frontend/`, `transform/dbt/models/marts/` |
| **Kết luận** | Kết quả đạt được, ý nghĩa đề tài, hướng mở rộng tương lai | Tổng hợp Chương 1--5; không đưa thiết kế mới chưa triển khai |

---

## 9. Bản mẫu đã chuẩn hóa (tham chiếu nội bộ Agent)

Các file sau đã được sửa theo rule này; khi viết mục mới **bắt chước giọng
văn và mức độ chi tiết**, không copy path từ đó:

| File | Nội dung | Mức văn |
|---|---|---|
| `Docs/latex/ch1_revised.tex` | Chương 1 — tổng quan, lý thuyết | Thấp (~40\% văn); nhiều bảng/hình |


Khi rule và bản mẫu mâu thuẫn, **ưu tiên rule + codebase hiện tại**.
