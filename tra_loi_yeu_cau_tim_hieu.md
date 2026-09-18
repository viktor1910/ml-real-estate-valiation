# TRẢ LỜI YÊU CẦU TÌM HIỂU (Mục III) — BẢO VỆ & THUYẾT TRÌNH
## Đề tài: Dự báo & Định giá Bất động sản Tự động — Phát hiện tin định giá thấp

> Tài liệu trả lời **từng ý** trong Mục III của đề bài, bám source code thật + số liệu lần train gần nhất. Dùng kèm `thuyet_trinh_bao_ve_do_an.md` (kịch bản nói chi tiết theo file code).
>
> **Định vị đề tài (nói ngay đầu buổi):** Mục I cho chọn *một HQT NoSQL / một phần mềm hệ sinh thái Big Data* **HOẶC** *một hệ thống xử lý dữ liệu lớn theo thời gian thực*. Nhóm chọn **hệ thống xử lý theo thời gian thực = Apache Kafka + Apache Spark**. Vì thế các ý về "HQT NoSQL" trong Mục III mang ghi chú "(nếu có)" — nhóm **không dùng NoSQL** và giải thích lý do (ý 2, 3). PostgreSQL chỉ là tầng phục vụ (sink) cuối, không phải trọng tâm.

---

## Ý 1 — Phát biểu bài toán dữ liệu lớn & mô tả dữ liệu

### 1.1 Bài toán
- **Đầu vào:** đặc trưng một căn BĐS tại TP.HCM (diện tích, số phòng, hướng, loại hình, toạ độ lat/lon, nội thất…) + chỉ số vĩ mô tại **ngày đăng tin**.
- **Đầu ra:** `price_per_m2` — giá thực dự đoán (triệu VND/m²).
- **Ứng dụng:** so giá dự đoán với giá rao để phát hiện tin **định giá thấp**:
  ```
  undervalued_ratio = (predicted − listing) / predicted
  is_undervalued = undervalued_ratio ≥ 0.20      (rao thấp hơn dự đoán ≥ 20%)
  ```
- **Loại bài toán:** hồi quy (regression) có giám sát.
- **Phạm vi:** chỉ **TP.HCM**, chỉ phân khúc **bán** (loại thuê).
- **Vì sao target là giá/m² chứ không phải tổng giá?** Chuẩn hoá theo diện tích → giảm phương sai, căn 30m² và 300m² so sánh được.

### 1.2 Vì sao là Big Data — lập luận 3V
| V | Thể hiện trong hệ thống |
|---|---|
| **Volume** | Cào chotot liên tục, tích lũy **Parquet data lake** phân vùng theo ngày (`dt=YYYY-MM-DD`), lớn dần không giới hạn theo thời gian |
| **Velocity** | **Kafka** làm buffer: scraper (producer) đẩy tin không phải chờ Spark xử lý xong; Spark đọc theo lô `trigger(availableNow=True)` |
| **Variety** | BĐS (text tiêu đề, số phòng, toạ độ) **+** vĩ mô nhịp ngày (VN-Index, USD/VND, giá vàng) **+** vĩ mô nhịp tháng (CPI, lãi suất) |

> Trung thực: dữ liệu hiện ~443 dòng (cửa sổ ngắn ~7 tuần). Đây là **thiết kế đúng cho Big Data về kiến trúc** (pipeline scale được), không phải Big Data về khối lượng hiện tại — trả lời thẳng nếu bị hỏi (xem ý 7 & phần hạn chế).

### 1.3 Nguồn & mô tả dữ liệu
| Nguồn | Vai trò | Ghi chú |
|---|---|---|
| **chotot gateway API** (`gateway.chotot.com/v1/public/ad-listing`) | Tin BĐS chính | JSON public, không cần key; lọc `region_v2=13000` = HCM ngay tại API |
| **vnstock** (VCI/VCB/SJC) | Vĩ mô nhịp ngày: VN-Index, USD/VND, vàng | Fail → carry-forward, không vỡ pipeline |
| **CSV nhập tay** (`cpi_rate_monthly.csv`) | CPI + lãi suất nhịp tháng | Deterministic → việc code làm được thì không gọi API |

- **Schema:** key tiếng Anh, value tiếng Việt, xuyên suốt 22 cột.
- **Đặc điểm dữ liệu thật:** nhiều cột null (số tầng, phòng ngủ, nội thất) → xử lý bằng impute có kiểm soát (ý 7).

---

## Ý 2 — Thông tin chung về công cụ sử dụng (NoSQL "(nếu có)")

> **NoSQL: KHÔNG dùng — có chủ đích.** Output cuối là bảng schema cố định cần truy vấn phân tích (`WHERE is_undervalued`, `GROUP BY district`) → **PostgreSQL (quan hệ)** đúng hơn. Nhồi NoSQL vào đây không mang lợi ích → chọn đúng công cụ thay vì chạy theo buzzword. **Công cụ nghiên cứu chính = Kafka + Spark** (đều thuộc hệ sinh thái Big Data / real-time).

### 2.1 Apache Kafka — nền tảng streaming phân tán
- **Bản chất:** hàng đợi message phân tán, log append-only, lưu bền theo offset, replay được.
- **Vai trò trong hệ thống:** buffer giữa scraper (producer) và Spark (consumer); topic `real_estate_raw`.
- **Kiến trúc:** broker + topic + partition (scale ngang); bản dùng ở đây là **Kafka 3.7 KRaft** (bỏ Zookeeper).

### 2.2 Apache Spark — engine xử lý dữ liệu lớn
- **Bản chất:** engine tính toán phân tán in-memory; trừu tượng `RDD → DataFrame`; lazy evaluation + DAG scheduler.
- **Module dùng:**
  - **Spark SQL / DataFrame** — ETL, làm sạch, feature engineering.
  - **Spark MLlib `Pipeline`** — gói feature stages + regressor thành **1 artifact** (train/serve không lệch).
  - **Spark Structured Streaming** — đọc Kafka → ghi Parquet.
- **Chế độ chạy:** `local[*]` (đa nhân 1 máy) cho đồ án; kiến trúc chuyển cụm được không đổi code.

### 2.3 PostgreSQL — tầng phục vụ (sink)
- Lưu bảng `predictions` cho dashboard/truy vấn cuối (M9). Quan hệ, schema cố định.

---

## Ý 3 — Ưu/khuyết điểm & đặc điểm nổi bật so với sản phẩm cùng loại

### 3.1 Apache Kafka
- **Ưu:** tách rời tốc độ crawl khỏi tốc độ xử lý (velocity); bền — replay từ offset đầu; scale ngang qua partition; throughput hàng triệu msg/s.
- **Khuyết:** thêm 1 tầng vận hành; overkill nếu dữ liệu nhỏ.
- **Nổi bật hơn (vs RabbitMQ/ActiveMQ):** RabbitMQ hướng message queue truyền thống, xoá sau khi consume; **Kafka giữ log bền + replay + throughput cao hơn nhiều** → hợp streaming ingestion & event log.

### 3.2 Apache Spark
- **Ưu:** một engine cho **batch + streaming + ML**; API DataFrame cao cấp; `Pipeline` chống lệch train/serve; xử lý phân tán.
- **Khuyết:** nặng JVM, tốn RAM; overkill với dữ liệu nhỏ (trung thực — xem ý 7).
- **Nổi bật hơn (vs Hadoop MapReduce):** MapReduce ghi đĩa mỗi bước → chậm; **Spark tính in-memory nhanh hơn 10–100×**, lại có sẵn MLlib + Streaming trong cùng framework (Hadoop cần Mahout/Storm riêng).

### 3.3 PostgreSQL (vs NoSQL cho bài toán này)
- **Ưu (hợp bài toán):** ACID, SQL phân tích mạnh, join/aggregate/index sẵn — hợp output bảng phẳng cần `GROUP BY district`, lọc `is_undervalued`.
- **Vì sao không NoSQL:** MongoDB/Cassandra mạnh khi schema linh hoạt / ghi phân tán khối lượng cực lớn — **không phải nhu cầu ở tầng phục vụ này**; dùng vào sẽ mất khả năng truy vấn phân tích quan hệ mà không được gì lại.

---

## Ý 4 — Case study thực tế

| Công cụ | Áp dụng thực tế |
|---|---|
| **Kafka** | **LinkedIn** (nơi sinh ra Kafka — theo dõi hoạt động người dùng), **Uber** (pipeline sự kiện chuyến đi), **Netflix** (thu thập log/telemetry hàng triệu event/s) |
| **Spark** | **Alibaba** (ETL + đề xuất trên petabyte), **Shopify** (phân tích thương mại điện tử), nhiều **ngân hàng** (ETL + scoring rủi ro hàng tỷ dòng) |
| **PostgreSQL** | Chuẩn công nghiệp cho OLTP/analytics; **Instagram, Reddit** dùng ở quy mô lớn |

> Liên hệ đề tài: bài toán "định giá thấp" đúng dạng pipeline **ingest sự kiện (Kafka) → xử lý/ML (Spark) → phục vụ truy vấn (RDBMS)** mà các công ty trên dùng, chỉ khác quy mô.

---

## Ý 5 — Mô hình hệ thống

```
[Scrapy chotot API]
       │ JSON stream
       ▼
[Kafka: real_estate_raw]          ← buffer streaming (velocity)
       │  (replay demo: streaming/replay_producer.py)
       ▼
[Spark Structured Streaming]      streaming/kafka_to_parquet.py
       │ trigger(availableNow=True) + checkpoint
       ▼
data/lake/listings_raw (Parquet, dt=…)      ← RAW landing
       │
       ▼
[clean_parse.py]  parse + lọc scope + GIỮ null → listings_pool (append dt)
       │
   ┌───┴───────────────────────────────────────────────┐
   │ LUỒNG A — RETRAIN (đắt, chỉ khi drift)             │
   │  impute_fit → impute_apply(train) → feature_pipeline│
   │  → train (3 model + CV + ablation) → promote-if-better│
   │      → models/v<date>/model  (FULL PipelineModel)  │
   └───┬───────────────────────────────────────────────┘
       │ LUỒNG B — SERVE (rẻ, hàng ngày)
       ▼
[fetch_macro] → [impute_apply(serve)] → [score_new] → predictions/sale
       │                                   │ ghi cờ drift → retrain_needed.json
       │                                   ▼
       │                         nếu drift ⇒ gọi lại LUỒNG A
       ▼
[PostgreSQL: predictions] → [Dashboard / báo cáo]
```

**3 nguyên lý cốt lõi (điểm nhấn bảo vệ):**
1. **Tách 2 luồng:** Train (Luồng A) đắt, chỉ chạy khi **drift**; Serve (Luồng B) rẻ, hàng ngày, chỉ `load model → transform`.
2. **Tách tín hiệu / hành động:** `score_new` chỉ **ghi cờ** drift, không tự train; orchestrator mới quyết định retrain.
3. **Promote-if-better:** model mới chỉ lên prod nếu RMSE ≤ model cũ, ngược lại giữ cũ (chống hồi quy chất lượng).

---

## Ý 6 — Cài đặt, kết nối Spark & điều chỉnh tham số

### 6.1 Môi trường
```bash
cd "/Users/viktornguyen/Desktop/viktor/Machine learning"
source .venv/bin/activate
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
```
- Python 3.14, **PySpark 4.2.0**, Scrapy 2.19, vnstock, pandas. Java **OpenJDK 17** (bắt buộc cho Spark).

### 6.2 Hạ tầng Docker
```bash
docker compose up -d      # Kafka 3.7 (KRaft) + PostgreSQL 15
```
- Kafka cổng 9092, auto-create topic. Postgres `realestate/admin`, tự chạy `sql/init.sql` tạo bảng `predictions`.

### 6.3 Kết nối Spark & tham số (ngay trong code)
| Tham số | Giá trị | Lý do |
|---|---|---|
| `master` | `local[*]` | chạy local đa nhân |
| `spark.sql.shuffle.partitions` | `8` (mặc định 200) | dữ liệu nhỏ → giảm shuffle, nhanh hơn |
| `spark.jars.packages` | `spark-sql-kafka-0-10_2.12:3.5.0` | connector đọc Kafka |
| Streaming | `trigger(availableNow=True)` + `checkpointLocation` | đọc hết offset mới rồi dừng (batch, không stream vô tận) |
| ML tuning | `CrossValidator(numFolds=3, parallelism=2)` + `ParamGridBuilder` | `regParam/elasticNet` (LR); `numTrees/maxDepth` (RF); `maxDepth/maxIter` (GBT) |

- **Ghi chú kỹ thuật (đáng nói):** PySpark 4.2 **không cast DATE→INT** → dùng `F.unix_date()` cho window `rangeBetween`.

---

## Ý 7 — Minh hoạ xử lý dữ liệu lớn (số liệu THẬT)

### 7.1 Kết quả model (test holdout, target = triệu VND/m²)
| Model | RMSE | MAE | R² |
|---|---|---|---|
| **RandomForest** ✅ | **25.69** | 21.16 | **0.744** |
| GBT | 28.40 | 22.66 | 0.687 |
| LinearRegression | 37.61 | 29.24 | 0.451 |

- **Model sản phẩm: RandomForest**, R² ≈ 0.74 (giải thích ~74% phương sai giá/m²).
- Train 342 / test 58 dòng (random 80/20, seed=42); tổng 400 dòng sau lọc IQR (từ 443 dòng dedup).

### 7.2 Chống rò rỉ dữ liệu (leakage) — điểm nhấn kỹ thuật
- Vĩ mô ghép **as-of join** theo `posted_date` (ngày ≤ hôm đăng) + cửa sổ **trailing** (90d MA/%; CPI/lãi suất lag 1/3/6 tháng, min lag 1 tháng).
- `rank_quan`: tier thị trường **hard-map 1–5** theo quận (không học từ giá → 0 leakage).
- Feature stages fit **chỉ trên train fold** trong CV.

### 7.3 GATE vĩ mô + Ablation — trung thực dữ liệu nhỏ
- **Gate:** chỉ bật 14 cột vĩ mô khi pool đủ sâu: `span ≥ 183 ngày (≈6 tháng) VÀ ≥ 3 tháng đăng phân biệt`. Dưới ngưỡng → **drop hết cột vĩ mô** (tránh "nhiễu hằng số theo chiều ngang" — mọi dòng cùng 1 giá trị vĩ mô ⇒ vô dụng cho cây quyết định).
- **Ablation macro vs no_macro:** lần chạy hiện tại delta_rmse ≈ 0 vì cửa sổ ngắn → **gate = OFF** → 2 nhánh trùng nhau. **Đây không phải bug** — gate làm đúng việc. Kiến trúc vĩ mô đã sẵn sàng, đủ ≥6 tháng dữ liệu sẽ tự bật.

### 7.4 Drift & phát hiện định giá thấp
- Drift check gần nhất: `batch_rmse = 28.13 ≤ ngưỡng 38.53` (= 1.5 × baseline) → **không drift**, giữ model.
- Output `predictions/sale`: mỗi tin có `predicted_ppm2`, `undervalued_ratio`, `is_undervalued`, `predicted_total_vnd`; demo top-15 tin định giá thấp theo quận.
- Đã nạp **443 dòng** predictions vào PostgreSQL (M9), kiểu dữ liệu + số dòng verify đúng.

### 7.5 Kịch bản demo trực tiếp
1. `docker compose up -d` → show Kafka + Postgres chạy.
2. `python ml/fetch_macro.py` → lake vĩ mô.
3. `bash ml/run_daily.sh` → chạy hết luồng serve, in cờ drift.
4. Query bảng `predictions` → show top tin định giá thấp.
5. (Tuỳ) `bash ml/retrain.sh` → show promote-if-better giữ/đổi model.

---

## HẠN CHẾ & PHẢN BIỆN (chuẩn bị câu hỏi khó)
- **Dữ liệu nhỏ (~400 dòng):** Spark "overkill" về hiệu năng nhưng **đúng yêu cầu công cụ Big Data** về kiến trúc; cần tích lũy pool nhiều tháng để gate vĩ mô bật.
- **Vì sao random split, không split theo thời gian?** Nói thẳng: đây là **giải pháp tạm do data hiện ít**, KHÔNG phải random đúng nguyên tắc hơn. Về lý thuyết, data có yếu tố thời gian **nên split theo thời gian** (train quá khứ → test tương lai) để mô phỏng đúng cách model dùng thực tế và chống leakage tương lai→quá khứ. Nhưng hiện data chỉ ~400 dòng dồn trong cửa sổ ~7 tuần → cắt theo thời gian để lại test quá nhỏ/mất cân đối, chỉ số không đủ tin cậy. Vì vậy tạm dùng random 80/20 (test ~58 dòng) để có đánh giá ổn định. **Khi pool tích lũy đủ nhiều tháng → chuyển sang time-based split** (đúng chuẩn cho dữ liệu chuỗi thời gian). Đây là đánh đổi có ý thức, không phải lựa chọn tối ưu vĩnh viễn.
- **Kafka/crawl live đang comment** trong `run_daily` (demo tái dùng data) — bật khi hạ tầng ổn.
- **Hướng mở rộng:** đặc trưng ảnh/mô tả (NLP), nhiều tỉnh, chuyển Spark local → cụm, dashboard (M10).
