# THUYẾT TRÌNH BẢO VỆ ĐỒ ÁN
## Dự báo & Định giá Bất động sản Tự động — Phát hiện tin đăng bị định giá thấp (Real Estate Valuation)

> Tài liệu này bám sát **source code thật** và **luồng chạy thật** của hệ thống, kèm số liệu thực tế từ lần train gần nhất. Dùng làm kịch bản nói + slide nội dung khi bảo vệ.

---

## PHẦN 0. TÓM TẮT 30 GIÂY (mở đầu)

> "Hệ thống cào tin bất động sản HCM hàng ngày, kết hợp chỉ số vĩ mô, huấn luyện mô hình hồi quy dự đoán **giá thực trên m²** của từng căn, rồi so với giá rao để phát hiện tin **đang bị định giá thấp** — công cụ cho nhà đầu tư săn deal. Toàn bộ chạy trên nền Big Data: **Kafka** (streaming buffer) → **Spark SQL + MLlib** (ETL + học máy) → **PostgreSQL** (phục vụ)."

**Đề bài chọn:** hệ thống **xử lý dữ liệu lớn theo thời gian thực** (Kafka + Spark Structured Streaming), không phải NoSQL.

---

## PHẦN 1. PHÁT BIỂU BÀI TOÁN & MÔ TẢ DỮ LIỆU

### 1.1 Bài toán

- **Mục tiêu:** dự đoán `price_per_m2` (triệu VND/m²) của một bất động sản tại TP.HCM từ đặc trưng căn + chỉ số vĩ mô tại **ngày đăng tin**.
- **Ứng dụng:** với mỗi tin mới, so `predicted_ppm2` với `listing_ppm2` (giá rao) → tính:
  ```
  undervalued_ratio = (predicted - listing) / predicted
  is_undervalued = undervalued_ratio >= 0.20   (rao thấp hơn dự đoán ≥ 20%)
  ```
- **Phạm vi:** chỉ TP.HCM, chỉ phân khúc **bán** (loại thuê).

### 1.2 Lập luận 3V (chứng minh là bài toán Big Data)

| V | Thể hiện trong hệ thống |
|---|---|
| **Volume** | Cào chotot liên tục, tích lũy **Parquet data lake** phân vùng theo ngày (`dt=YYYY-MM-DD`), lớn dần vô hạn theo thời gian |
| **Velocity** | **Kafka** làm buffer — scraper đẩy tin không phải đợi Spark xử lý xong; Spark đọc theo lô `trigger(availableNow=True)` |
| **Variety** | BĐS (text tiêu đề, số phòng/diện tích, toạ độ lat/lon) **+** chuỗi vĩ mô nhịp ngày (VN-Index, USD/VND, vàng) **+** vĩ mô nhịp tháng (CPI, lãi suất) |

### 1.3 Nguồn dữ liệu

| Nguồn | Vai trò | Ghi chú |
|---|---|---|
| **chotot gateway API** (`gateway.chotot.com/v1/public/ad-listing`) | Nguồn tin BĐS chính | JSON public, không cần key. Lọc thẳng `region_v2=13000` = HCM |
| **vnstock** (VCI/VCB/SJC) | Vĩ mô nhịp ngày: VN-Index, USD/VND, giá vàng | Fail thì carry-forward, không vỡ pipeline |
| **CSV nhập tay** (`cpi_rate_monthly.csv`) | CPI + lãi suất nhịp tháng | Deterministic → không dùng API cho việc code làm được |

**Target:** `price_per_m2` thay vì tổng giá → chuẩn hoá theo diện tích, giảm phương sai (căn 30m² và 300m² so sánh được).

---

## PHẦN 2. CÔNG CỤ BIG DATA — GIỚI THIỆU & ĐÁNH GIÁ

### 2.1 Apache Kafka (streaming buffer)

- **Vai trò:** hàng đợi phân tán giữa scraper (producer) và Spark (consumer). Topic `real_estate_raw`.
- **Ưu điểm:** tách rời tốc độ crawl khỏi tốc độ xử lý (velocity); bền (retention, replay được từ offset đầu); scale ngang qua partition.
- **Khuyết điểm:** thêm 1 tầng vận hành; overkill nếu dữ liệu nhỏ.
- **Case study thực tế:** LinkedIn (nơi sinh ra Kafka), Uber, Netflix dùng cho pipeline event hàng triệu msg/s.

### 2.2 Apache Spark — SQL + MLlib (bộ não xử lý)

- **Vai trò:** ETL (`Spark SQL`), feature engineering, huấn luyện & suy luận ML (`Spark MLlib` `Pipeline`), Structured Streaming (đọc Kafka → Parquet).
- **Ưu điểm:** một engine cho cả batch + streaming + ML; API `DataFrame` cao cấp; `Pipeline` gói feature + model thành **1 artifact** → train/serve không lệch.
- **Khuyết điểm:** nặng JVM, tốn RAM; overkill với dữ liệu nhỏ (xem mục 6 — trung thực).
- **Case study:** Alibaba, Shopify, ngân hàng dùng Spark cho ETL + scoring hàng tỷ dòng.

### 2.3 PostgreSQL (kho phục vụ)

- **Vai trò:** lưu bảng `predictions` cho dashboard/truy vấn cuối (M9).
- **Quyết định chốt:** dùng **quan hệ (PostgreSQL)**, KHÔNG NoSQL — vì output là bảng có schema cố định, cần truy vấn phân tích (`WHERE is_undervalued`, `GROUP BY district`). NoSQL không mang lại lợi ích ở đây → chọn đúng công cụ thay vì chạy theo buzzword.

> **Điểm nhấn khi bảo vệ:** đề cho chọn *NoSQL* HOẶC *hệ thống real-time*. Nhóm chọn **real-time (Kafka+Spark Streaming)** làm trọng tâm Big Data, PostgreSQL chỉ là sink.

---

## PHẦN 3. KIẾN TRÚC & MÔ HÌNH HỆ THỐNG

```
[Scrapy chotot API]                     scraper/realestate/spiders/chotot_spider.py
       │ JSON stream
       ▼
[Kafka: real_estate_raw]                docker-compose.yml (kafka)
       │ replay demo: streaming/replay_producer.py
       ▼
[Spark Structured Streaming]            streaming/kafka_to_parquet.py
       │ trigger(availableNow=True)
       ▼
data/lake/listings_raw  (Parquet, dt=…)          ← RAW landing
       │
       ▼
[clean_parse.py]  parse + lọc scope + GIỮ null   → data/lake/listings_pool (append dt)
       │
   ┌───┴─────────────────────────────────────────────┐
   │ LUỒNG A (RETRAIN, ml/retrain.sh)                 │
   │  impute_fit → impute_apply(train) → feature_     │
   │  pipeline → train → promote-if-better            │
   │      → models/v<date>/model  (FULL PipelineModel)│
   └───┬─────────────────────────────────────────────┘
       │ LUỒNG B (SERVE hàng ngày, ml/run_daily.sh)
       ▼
[fetch_macro] → [impute_apply(serve)] → [score_new] → data/lake/predictions/sale
       │                                    │ ghi cờ drift → models/retrain_needed.json
       │                                    ▼
       │                          nếu drift ⇒ gọi lại LUỒNG A
       ▼
[PostgreSQL: predictions]  →  [Dashboard / báo cáo]
```

**Nguyên lý cốt lõi — tách 2 luồng:**
- **Luồng A = Train** (đắt, thỉnh thoảng): chỉ chạy khi phát hiện **drift**.
- **Luồng B = Serve** (rẻ, hàng ngày): chỉ `load model → transform`, không train lại.
- **Score chỉ báo tín hiệu** (ghi cờ), **không tự train** → tách tín hiệu khỏi hành động.

---

## PHẦN 4. LUỒNG CHẠY THEO SOURCE CODE (phần lõi khi bảo vệ)

### 4.1 Thu thập — `scraper/realestate/spiders/chotot_spider.py`

- Gọi API `ad-listing` cho 4 nhóm BĐS (`cg = 1000/1010/1020/1030`), lọc `region_v2=13000` (HCM) ngay tại API → không cào toàn quốc.
- **Bug đã xử lý (đáng kể để nói):** chotot **bỏ qua param `page`**; phải phân trang bằng **offset** `o=(page-1)*limit`. Đã verify `page=1==page=2` nhưng `o=0 != o=20`.
- Map code→text: `CATEGORY_MAP`, `DIRECTION_MAP`… ; giá/diện tích/toạ độ; fallback bóc `floors`/`bedrooms` từ body HTML khi thiếu.
- Emit **key tiếng Anh, value tiếng Việt** (schema English xuyên suốt).

### 4.2 Streaming — `streaming/`

- `replay_producer.py`: đọc CSV cũ → map 22 cột schema → đẩy Kafka (demo khi chưa crawl live).
- `kafka_to_parquet.py`: `readStream` Kafka → parse JSON theo `RAW_SCHEMA` (mọi cột string, cast sau) → ghi Parquet `listings_raw` phân vùng `dt`, `trigger(availableNow=True)` (đọc hết offset rồi dừng — batch chứ không stream vô tận), có `checkpointLocation`.

### 4.3 Làm sạch giai đoạn 1 — `ml/clean_parse.py`

- Đọc `listings_raw` → cast kiểu (`price` double VND, `area` regex `"92 m²"→92.0`, `posted_at` timestamp…).
- **Lọc scope:** `city contains "Hồ Chí Minh"` · `ad_type='s'` (bán) · `price>0 & area>0` · `dropDuplicates(url)`.
- Tính target `price_per_m2 = (price/1e6)/area`.
- `rank_quan`: **tier thị trường cứng 1–5** theo quận (hard-map, **0 leakage** — không học từ giá). Quận lạ → null + cảnh báo.
- **GIỮ null, KHÔNG impute, KHÔNG IQR** ở bước này → `append` partition `dt` vào `listings_pool`. (Impute/IQR để dành lúc retrain để stats luôn tươi.)

### 4.4 Học tham số làm sạch — `ml/impute_fit.py` (chỉ khi retrain)

- Dedup toàn pool theo `url` (giữ bản `dt` mới nhất).
- Tính **IQR bounds** (`price_per_m2`, `area`) — ngưỡng lọc ngoại lai.
- Tính `fill`: `floors=1`, `bedrooms=median`, `interior='UNKNOWN'`.
- Ghi **đóng băng** ra `models/impute_stats.json`.
- *Số thật lần fit gần nhất:* 443 dòng dedup → **400 dòng sau IQR**; `bedrooms` median = 3.

### 4.5 Áp tham số — `ml/impute_apply.py`

- `--mode train`: dedup + **lọc IQR (drop ngoại lai)** + điền khuyết → data train.
- `--mode serve`: **CHỈ điền khuyết, GIỮ mọi dòng** (không vứt tin cần chấm).
- Cùng 1 file stats đóng băng → train/serve nhất quán.

### 4.6 Đặc trưng + ghép vĩ mô — `ml/feature_pipeline.py` + `ml/macro_features.py`

**`macro_features.py` — module vĩ mô dùng chung** (train + serve gọi cùng hàm → chống lệch):
- Đặc trưng vĩ mô **trailing** (chống leakage): thị trường 90d MA + 90d %; CPI/lãi suất lag 1/3/6 tháng (min lag = 1 tháng).
- **GATE (điểm nhấn kỹ thuật):** chỉ bật cột vĩ mô khi pool đủ sâu thời gian:
  ```
  span >= 183 ngày (≈6 tháng) VÀ >= 3 tháng đăng phân biệt
  ```
  Dưới ngưỡng → **drop hết cột vĩ mô** để tránh "nhiễu hằng số theo chiều ngang" (mọi dòng cùng 1 giá trị vĩ mô ⇒ vô dụng cho cây quyết định).
- `attach_macro()`: **as-of join** theo `posted_date` (ngày ≤ hôm đăng); dòng sau ngày macro cuối → carry-forward; `fillna` **null-safe** (chỉ điền cột có giá trị → tránh `NullPointerException` của PySpark 4.2 khi window rỗng).

**`feature_pipeline.py` — recipe Spark ML Pipeline:**
- `SQLTransformer` phái sinh: `log_area`, `year/month/quarter/dayofweek`, `dist_center` (Haversine tới trung tâm HCM), coalesce categorical.
- `StringIndexer + OneHotEncoder` cho `property_type` + `interior`.
- `VectorAssembler` + `StandardScaler`.
- **NUM động:** `NUM = NUM_BASE + macro_present` → gate OFF thì 0 cột vĩ mô, gate ON thì +14 cột. Không hard-code.

### 4.7 Huấn luyện + tuning + ablation — `ml/train.py`

- Đọc `listings_features/sale` → **random split 80/20 (seed=42)**.
  > *Vì sao random, không split theo thời gian?* Data dồn gần như 1 cửa sổ (chủ yếu 1 ngày/1 tháng), split-theo-năm cho test n≈14 → vô nghĩa thống kê. Random cho test ~58 dòng, chỉ số ổn định. Đánh đổi được ghi rõ trong comment (trung thực).
- 3 regressor: **LinearRegression / RandomForest / GBT**, mỗi cái `CrossValidator` 3-fold + `ParamGridBuilder` (minh hoạ "cách điều chỉnh tham số").
- **Ablation vĩ mô:** chạy 2 bộ `macro` vs `no_macro` (6 ô) để chứng minh giá trị vĩ mô.
- Chọn model tốt nhất (nhóm macro, RMSE test nhỏ nhất) → lưu **FULL `PipelineModel`** (feature stages fit trên train + regressor) `models/v<date>/model` + `metrics.json` (có `baseline_rmse` cho drift check).

### 4.8 Chấm điểm + drift — `ml/score_new.py`

- `attach_macro` lô mới → `PipelineModel.load` → `transform` → `predicted_ppm2`.
- Cờ `is_undervalued` (ratio ≥ 0.20) + `predicted_total_vnd`.
- **Drift check trung thực:** RMSE tính trên **test-holdout** (tái tạo `randomSplit` seed=42 y hệt train), KHÔNG trên full batch (full batch trùng data train ⇒ lạc quan giả). Drift khi `RMSE > 1.5 × baseline`.
- Ghi cờ ra `models/retrain_needed.json` (score chỉ báo, không tự train).

### 4.9 Thăng hạng model — `ml/promote.py`

- **Promote-if-better gate:** so `baseline_rmse` model mới vs model prod (`models/current.json`).
  - mới ≤ cũ → cập nhật con trỏ prod.
  - mới tệ hơn → **giữ cũ** (rollback tự nhiên, không đè mù).
- Deterministic, không Spark (đúng nguyên tắc: việc code làm được thì không dùng model).

### 4.10 Điều phối — `ml/run_daily.sh` (serve) & `ml/retrain.sh` (train)

`run_daily.sh` (cron 2h sáng):
```
0) fetch_macro          → macro_raw lake (fail → carry-forward)
1-2) crawl → Kafka → raw parquet   (bật khi Kafka chạy; demo comment)
3) clean_parse          → pool (append dt hôm nay)
4) impute_apply serve   → scored_input (giữ mọi tin)
5) score_new            → predictions + ghi cờ drift
6) đọc cờ → NẾU drift ⇒ bash retrain.sh   (promote-if-better)
```
`retrain.sh`: `impute_fit → impute_apply(train) → feature_pipeline → train → promote`.

---

## PHẦN 5. CÀI ĐẶT, KẾT NỐI SPARK & THAM SỐ

### 5.1 Môi trường
```bash
cd "/Users/viktornguyen/Desktop/viktor/Machine learning"
source .venv/bin/activate
export JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home
```
- Python 3.14, **PySpark 4.2.0**, Scrapy 2.19, vnstock, pandas. Java: **OpenJDK 17** (bắt buộc cho Spark).

### 5.2 Hạ tầng (Docker)
```bash
docker compose up -d          # Kafka 3.7 (KRaft, không Zookeeper) + PostgreSQL 15
```
- Kafka cổng 9092, auto-create topic. Postgres `realestate/admin`, tự chạy `sql/init.sql` tạo bảng `predictions`.

### 5.3 Kết nối & tham số Spark (trong code)
- Tạo session: `SparkSession.builder.master("local[*]")` — chạy local đa nhân.
- `spark.sql.shuffle.partitions=8` (giảm từ 200 mặc định → hợp dữ liệu nhỏ, nhanh hơn).
- Kafka connector: `spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0`.
- Streaming: `trigger(availableNow=True)` + `checkpointLocation` (đọc offset mới rồi dừng).
- ML tuning: `CrossValidator(numFolds=3, parallelism=2)`, `ParamGridBuilder` (regParam/elasticNet cho LR; numTrees/maxDepth cho RF; maxDepth/maxIter cho GBT).
- Ghi chú kỹ thuật: PySpark 4.2 **không cast DATE→INT** → dùng `F.unix_date()` cho window `rangeBetween`.

---

## PHẦN 6. MINH HOẠ KẾT QUẢ (số liệu THẬT + trung thực)

### 6.1 Bảng chỉ số (test holdout, target = triệu VND/m²)

| Model | RMSE | MAE | R² |
|---|---|---|---|
| **RandomForest** ✅ | **25.69** | 21.16 | **0.744** |
| GBT | 28.40 | 22.66 | 0.687 |
| LinearRegression | 37.61 | 29.24 | 0.451 |

- **Model sản phẩm: RandomForest**, R² ≈ 0.74 (giải thích ~74% phương sai giá/m²).
- Train 342 / test 58 dòng (random 80/20 seed=42), tổng 400 dòng sau IQR.

### 6.2 Ablation vĩ mô — **điểm trung thực quan trọng**

- Ở lần chạy hiện tại: cột `macro` và `no_macro` cho chỉ số **y hệt** (delta_rmse ≈ 0).
- **Lý do (phải nói thẳng):** data crawl mới ~443 dòng dồn trong cửa sổ ngắn → **GATE = OFF** (chưa đủ 6 tháng & 3 tháng) → toàn bộ cột vĩ mô bị drop theo thiết kế → 2 nhánh ablation trùng nhau. Đây **không phải bug**, mà là gate làm đúng việc chống nhiễu hằng số.
- **Thông điệp:** kiến trúc vĩ mô đã sẵn sàng; khi pool tích lũy đủ ≥6 tháng, gate tự bật 14 cột và ablation mới có ý nghĩa. Báo cáo trung thực theo số hiện có thay vì thổi phồng.

### 6.3 Drift & phát hiện định giá thấp
- Drift check gần nhất: `batch_rmse=28.13 ≤ ngưỡng 38.53` → **không drift**, giữ model.
- Output `predictions/sale`: mỗi tin có `predicted_ppm2`, `undervalued_ratio`, `is_undervalued`, top-15 định giá thấp theo quận (demo trên slide).

### 6.4 Kịch bản demo trực tiếp (đề xuất trình chiếu)
1. `docker compose up -d` → show Kafka + Postgres chạy.
2. `python ml/fetch_macro.py` → lake vĩ mô.
3. `bash ml/run_daily.sh` → chạy hết luồng serve, in cờ drift.
4. Mở `predictions` → show top tin định giá thấp.
5. (Tuỳ) `bash ml/retrain.sh` → show promote-if-better giữ/đổi model.

---

## PHẦN 7. ĐIỂM MẠNH THIẾT KẾ (chốt bảo vệ)

1. **Chống leakage triệt để:** vĩ mô as-of join + trailing window; `rank_quan` hard-map không học từ giá; feature stages fit **chỉ trên train fold** trong CV.
2. **Train/serve nhất quán:** một `PipelineModel` gói cả feature + model; module `macro_features` dùng chung.
3. **Tách tín hiệu/hành động:** score ghi cờ, orchestrator quyết retrain; promote-if-better chống hồi quy chất lượng.
4. **Trung thực dữ liệu nhỏ:** gate vĩ mô, ablation báo cáo đúng số, giải thích đánh đổi split — không thổi phồng.
5. **Đúng công cụ:** Kafka cho velocity, Spark cho batch+ML, Postgres cho serve; không nhồi NoSQL vô nghĩa.

## PHẦN 8. HẠN CHẾ & HƯỚNG PHÁT TRIỂN (chuẩn bị câu hỏi phản biện)

- **Dữ liệu nhỏ** (~400 dòng train) → Spark "overkill" hiệu năng nhưng đúng yêu cầu công cụ Big Data; cần tích lũy pool nhiều tháng để gate vĩ mô bật.
- **Kafka/crawl live đang comment** trong `run_daily` (demo tái dùng data) — cần bật khi hạ tầng chạy ổn.
- Chưa nạp `predictions` vào Postgres tự động (M9) & dashboard (M10) — DDL đã sẵn `sql/init.sql`.
- Hướng mở rộng: thêm đặc trưng ảnh/mô tả (NLP), mở nhiều tỉnh, chuyển Spark local → cụm.

---

### PHỤ LỤC — BẢN ĐỒ FILE ↔ CHỨC NĂNG

| File | Vai trò |
|---|---|
| `scraper/realestate/spiders/chotot_spider.py` | Cào chotot API (offset paging, map code→text) |
| `streaming/replay_producer.py` | CSV → Kafka (demo) |
| `streaming/kafka_to_parquet.py` | Kafka → Parquet raw (Structured Streaming) |
| `ml/clean_parse.py` | Parse + lọc scope + giữ null → pool |
| `ml/impute_fit.py` | Học IQR + fill stats (đóng băng JSON) |
| `ml/impute_apply.py` | Áp stats: train (lọc) / serve (giữ) |
| `ml/macro_features.py` | Vĩ mô trailing + GATE + as-of join (dùng chung) |
| `ml/feature_pipeline.py` | Recipe Spark ML Pipeline + join vĩ mô |
| `ml/train.py` | Train 3 model + CV + ablation + lưu versioned |
| `ml/score_new.py` | Score lô mới + cờ định giá thấp + cờ drift |
| `ml/promote.py` | Promote-if-better gate |
| `ml/fetch_macro.py` | Tải vĩ mô vnstock (daily + --backfill) |
| `ml/run_daily.sh` / `ml/retrain.sh` | Điều phối serve / train |
| `sql/init.sql`, `docker-compose.yml` | Hạ tầng Postgres + Kafka |
