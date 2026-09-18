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
| **Bảng Postgres `macro_monthly`** | CPI + lãi suất nhịp tháng | **THAY cho CSV nhập tay cũ.** Nhập/sửa qua **dashboard** (UPSERT theo tháng); `macro_features.load_macro()` đọc bảng này qua **JDBC**, không còn đọc CSV |

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

### 2.3 PostgreSQL (kho phục vụ + nguồn vĩ mô + auth)

- **Vai trò (đã mở rộng):** không còn là "sink thuần". Nay giữ **3 bảng** (`sql/init.sql`):
  - `predictions` — output đã chấm (M9 đã hoàn thành, `ml/load_predictions.py` append vào).
  - `macro_monthly` — CPI + lãi suất nhịp tháng (`month` PK), **thay CSV nhập tay**; Spark đọc qua JDBC lúc feature/score, dashboard UPSERT lúc nhập tay.
  - `users` — tài khoản đăng nhập dashboard (`password_hash` = bcrypt, không lưu plaintext).
- **Quyết định chốt:** dùng **quan hệ (PostgreSQL)**, KHÔNG NoSQL — vì output là bảng có schema cố định, cần truy vấn phân tích (`WHERE is_undervalued`, `GROUP BY district`) + UPSERT theo khoá (`month`, `username`). NoSQL không mang lại lợi ích ở đây → chọn đúng công cụ thay vì chạy theo buzzword.

> **Điểm nhấn khi bảo vệ:** đề cho chọn *NoSQL* HOẶC *hệ thống real-time*. Nhóm chọn **real-time (Kafka+Spark Streaming)** làm trọng tâm Big Data; PostgreSQL là tầng phục vụ + nguồn cấu hình vĩ mô.

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
       │ LUỒNG B (SERVE hàng ngày, run_daily.sh — ở project root)
       ▼
[fetch_macro] → [impute_apply(serve)] → [score_new] → data/lake/predictions/sale
       │                                    │ ghi cờ drift → models/retrain_needed.json
       │                                    ▼                         │
       │                          nếu drift ⇒ gọi lại LUỒNG A         │ [load_predictions.py]
       ▼                                                              ▼
[PostgreSQL]  ── predictions ─────────────────────────────►  APPEND bảng predictions
     ▲  │
     │  └── macro_monthly (CPI/lãi suất) ──JDBC──► feature_pipeline / score_new (as-of join)
     │
     └── [Dashboard Streamlit]  (auth bcrypt)  ── nhập/sửa CPI (UPSERT) + xem báo cáo
              └── public demo qua Cloudflare quick tunnel (dashboard/run.sh)
```

**Nguyên lý cốt lõi — tách 2 luồng:**
- **Luồng A = Train** (đắt, thỉnh thoảng): chỉ chạy khi phát hiện **drift**.
- **Luồng B = Serve** (rẻ, hàng ngày): chỉ `load model → transform`, không train lại.
- **Score chỉ báo tín hiệu** (ghi cờ), **không tự train** → tách tín hiệu khỏi hành động.
- **Postgres là trục 2 chiều:** vừa là *sink* (predictions) vừa là *source* (macro_monthly đọc ngược vào feature/score) vừa là *store auth* (users).

**Cấu hình & lưu trữ tách tầng — `ml/config.py` (nguồn cấu hình DUY NHẤT):** mọi path/master/JAVA_HOME/Postgres đọc từ env, **mặc định = hành vi local cũ**. Prod override qua `.env` để chạy phân tán: LAKE + MODEL_STORE lên `s3a://` (MinIO/S3, executor truy cập), META_DIR (JSON nhỏ, chỉ driver `open()`) ở volume local. Chi tiết deploy: [DEPLOY.md](DEPLOY.md).

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
- **Nguồn vĩ mô 2 tầng:** thị trường nhịp ngày đọc từ **Parquet lake** (`macro_raw`, do `fetch_macro` ghi); CPI/lãi suất nhịp tháng đọc từ **bảng Postgres `macro_monthly`** qua JDBC (`config.pg_read`) — **không còn CSV**.
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

### 4.10 Điều phối — `run_daily.sh` (serve, ở project root) & `ml/retrain.sh` (train)

`run_daily.sh` (cron 2h sáng; prod: `docker compose run --rm ml-app bash run_daily.sh`):
```
0) fetch_macro          → macro_raw lake (fail → carry-forward)
1) replay_producer      → Kafka (mặc định replay CSV seed; crawl thật bật khi spider verify)
2) kafka_to_parquet     → raw parquet landing (availableNow, checkpoint tích lũy)
3) clean_parse          → pool (append dt hôm nay)
4) impute_apply serve   → scored_input (giữ mọi tin)
5) score_new            → predictions + ghi cờ drift
6) đọc cờ → NẾU drift ⇒ bash ml/retrain.sh   (promote-if-better)
7) load_predictions     → APPEND bảng Postgres predictions (non-fatal nếu DB down)
```
- **Bước Kafka (1-2) nay CHẠY thật** trong daily loop (không còn comment), có fallback `|| echo` nếu Kafka/DB down → pipeline không vỡ. Nguồn tin mặc định là replay CSV seed cho ổn định demo; crawl live chỉ cần bật 1 dòng khi spider verify xong.

`ml/retrain.sh`: `impute_fit → impute_apply(train) → feature_pipeline → train → promote`.

### 4.11 Nạp kho phục vụ (M9) — `ml/load_predictions.py`

- Đọc `predictions/sale` (parquet output của score) → cast `id`/`scored_at` khớp `sql/init.sql` → **APPEND** vào bảng Postgres `predictions` qua JDBC.
- **Append (không dedup):** giữ lịch sử theo `scored_at` + `model_version` → dashboard chọn được từng đợt chấm.
- JDBC jar tự tải qua `spark.jars.packages=org.postgresql:postgresql:42.7.4`. Non-fatal trong `run_daily` → DB down không chặn pipeline.

### 4.12 Tầng trình bày — Dashboard Streamlit (`dashboard/`)

- **2 trang** (`dashboard/Home.py` + `dashboard/pages/1_CPI_Entry.py`), UI **tiếng Việt**:
  - **Báo cáo dự đoán:** chọn đợt chấm → metric (số dòng, % định giá thấp, giá TB) + lọc theo quận/cờ + 3 biểu đồ (giá thực vs dự đoán theo quận; đếm định giá thấp; phân bố % chênh lệch).
  - **Nhập CPI/lãi suất:** form UPSERT bảng `macro_monthly` (prefill tháng đã có = chỉnh sửa; tháng mới = thêm) — đây là cách nhập vĩ mô thay CSV.
- **Xác thực bắt buộc (`dashboard/auth.py`):** mọi trang gọi `require_login()` trước khi render; mật khẩu **bcrypt** trong bảng `users`; tạo user bằng `dashboard/manage_users.py`. Lý do: demo mở qua tunnel công khai → chặn người lạ ghi CPI gây nhiễu pipeline.
- **Demo công khai:** `dashboard/run.sh` chạy Streamlit + **Cloudflare quick tunnel** (`*.trycloudflare.com`) → chia sẻ URL không cần deploy server.
- **Tái dùng cấu hình:** `dashboard/db.py` import thẳng `PG_*` từ `ml/config.py` → không nhân đôi thông tin kết nối.

---

## PHẦN 5. CÀI ĐẶT, KẾT NỐI SPARK & THAM SỐ

### 5.1 Môi trường
```bash
cd "/Users/viktornguyen/Desktop/viktor/Machine learning"
source .venv/bin/activate
# JAVA_HOME nay tự dò trong ml/config.py (mac homebrew vs Linux) — export chỉ khi cần ghi đè
```
- Dev: **Python 3.14** (`.venv`). Container prod: **Python 3.11** (ổn định + tương thích pyspark 4.2 tốt hơn — pin trong `requirements.txt`).
- **PySpark 4.2.0**, Scrapy 2.19, vnstock, pandas, streamlit, psycopg2, bcrypt. Java: **OpenJDK 17** (bắt buộc cho Spark).

### 5.2 Hạ tầng (Docker)
```bash
# Dev local (chỉ hạ tầng phụ trợ):
docker compose up -d kafka postgres    # Kafka 3.7 (KRaft, không Zookeeper) + PostgreSQL 15
# Prod phân tán (thêm object store + cụm Spark):
docker compose up -d minio kafka postgres spark-master
docker compose up -d --scale spark-worker=2 spark-worker
```
- Kafka cổng 9092, auto-create topic. Postgres `realestate/admin`, tự chạy `sql/init.sql` tạo 3 bảng (`predictions`, `macro_monthly` + seed CPI, `users`).
- Prod thêm **MinIO** (S3 tương thích) làm shared storage cho lake + model, và **cụm Spark standalone** (master + N worker). `docker-compose.yml` đã gắn `platform: linux/arm64` cho máy Apple Silicon. Chi tiết: [DEPLOY.md](DEPLOY.md).

### 5.3 Kết nối & tham số Spark (trong code)
- Tạo session: mọi job dùng chung `config.build_spark(app_name, packages=…)` — **không còn hardcode**. `master` = env `SPARK_MASTER` (default `local[*]` cho dev; prod = `spark://spark-master:7077`). Tự bật cấu hình **S3A (MinIO)** khi path là `s3a://`.
- `spark.sql.shuffle.partitions` = env (default 8 local, 16 prod) — giảm từ 200 mặc định → hợp dữ liệu nhỏ.
- **Bộ jar (`SPARK_JARS_PACKAGES`, prod .env):** `hadoop-aws:3.5.0` + `spark-sql-kafka-0-10_2.13:4.2.0` + `postgresql:42.7.4`.
  > ⚠️ **Xung đột phiên bản đã chốt (Rule 7):** tài liệu cũ từng ghi connector Kafka `..._2.12:3.5.0`. **Sai** với stack hiện tại — PySpark 4.2 dùng Scala **2.13** + Hadoop bundle **3.5.0**, nên đúng phải là `spark-sql-kafka-0-10_2.13:4.2.0` và `hadoop-aws:3.5.0` (khớp bundle). Đã theo giá trị trong `.env.example` (mới hơn, đã test build).
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
1. `docker compose up -d kafka postgres` → show Kafka + Postgres chạy.
2. `python ml/fetch_macro.py` → lake vĩ mô.
3. `bash run_daily.sh` → chạy hết luồng serve (Kafka → parquet → clean → score → nạp Postgres), in cờ drift.
4. `bash dashboard/manage_users.py <user>` (1 lần) → tạo tài khoản; `bash dashboard/run.sh` → mở dashboard qua Cloudflare tunnel.
5. Trên dashboard: đăng nhập → **trang Báo cáo** show top tin định giá thấp + biểu đồ; **trang Nhập CPI** UPSERT 1 tháng vĩ mô mới.
6. (Tuỳ) `bash ml/retrain.sh` → show promote-if-better giữ/đổi model.
7. (Tuỳ) show deploy phân tán: `docker compose up -d minio spark-master && docker compose up -d --scale spark-worker=2 spark-worker` → Spark UI `:8080` thấy worker ALIVE.

---

## PHẦN 7. ĐIỂM MẠNH THIẾT KẾ (chốt bảo vệ)

1. **Chống leakage triệt để:** vĩ mô as-of join + trailing window; `rank_quan` hard-map không học từ giá; feature stages fit **chỉ trên train fold** trong CV.
2. **Train/serve nhất quán:** một `PipelineModel` gói cả feature + model; module `macro_features` dùng chung.
3. **Tách tín hiệu/hành động:** score ghi cờ, orchestrator quyết retrain; promote-if-better chống hồi quy chất lượng.
4. **Trung thực dữ liệu nhỏ:** gate vĩ mô, ablation báo cáo đúng số, giải thích đánh đổi split — không thổi phồng.
5. **Đúng công cụ:** Kafka cho velocity, Spark cho batch+ML, Postgres cho serve; không nhồi NoSQL vô nghĩa.
6. **Sẵn sàng phân tán không đổi code:** `ml/config.py` resolver — mọi path/master/JAVA_HOME/Postgres đọc env, default = local; prod chỉ đổi `.env` để chuyển sang cụm Spark + MinIO/S3 (executor đọc lake qua `s3a://`, không cần đĩa chung). Có Docker image + DEPLOY.md multi-node.

## PHẦN 8. HẠN CHẾ & HƯỚNG PHÁT TRIỂN (chuẩn bị câu hỏi phản biện)

- **Dữ liệu nhỏ** (~400 dòng train sau IQR) → Spark "overkill" hiệu năng nhưng đúng yêu cầu công cụ Big Data; cần tích lũy pool nhiều tháng để **gate vĩ mô bật** (hiện GATE=OFF).
- **Crawl live thật vẫn đang tắt** trong `run_daily` (mặc định replay CSV seed cho ổn định) — Kafka/streaming đã chạy thật; chỉ cần bật 1 dòng `scrapy crawl` khi spider verify xong.
- **M9 (nạp Postgres) & dashboard (M10) ĐÃ HOÀN THÀNH** — `load_predictions.py` + Streamlit 2 trang có auth + Cloudflare tunnel. (Ghi chú lịch sử: tài liệu bản trước liệt kê 2 mục này là "chưa làm".)
- Đăng nhập dùng session Streamlit (không token/hết hạn) + tunnel demo — đủ cho bảo vệ, chưa phải hạ tầng auth production.
- Hướng mở rộng: thêm đặc trưng ảnh/mô tả (NLP), mở nhiều tỉnh, chạy cụm Spark đa máy vật lý thật (DEPLOY.md mục 5 đã mô tả).

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
| `ml/load_predictions.py` | **M9**: nạp predictions parquet → Postgres (append) |
| `ml/config.py` | **Resolver DUY NHẤT**: path/master/JAVA_HOME/Postgres/S3A từ env |
| `run_daily.sh` (root) / `ml/retrain.sh` | Điều phối serve / train |
| `dashboard/Home.py` | Trang báo cáo dự đoán (metric + lọc + 3 biểu đồ) |
| `dashboard/pages/1_CPI_Entry.py` | Trang nhập/sửa CPI–lãi suất (UPSERT `macro_monthly`) |
| `dashboard/db.py` | Helper Postgres (query + UPSERT), tái dùng `config.PG_*` |
| `dashboard/auth.py` / `manage_users.py` | Login gate bcrypt / CLI tạo user |
| `dashboard/run.sh` | Chạy Streamlit + Cloudflare quick tunnel |
| `sql/init.sql` | DDL 3 bảng: `predictions`, `macro_monthly` (+seed CPI), `users` |
| `docker-compose.yml`, `Dockerfile`, `.env.example` | Hạ tầng: Kafka + Postgres + MinIO + cụm Spark |
| `DEPLOY.md` | Hướng dẫn deploy Linux phân tán (single-host + multi-node) |
