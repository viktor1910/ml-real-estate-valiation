# Đồ án: Dự báo xu hướng giá bất động sản — Kế hoạch Milestone

> **Kiến trúc:** `Dataset BĐS + vĩ mô (vàng/USD-VND/VN-Index) → Kafka → Spark batch (SQL + MLlib) → Parquet staging → PostgreSQL`
>
> **Ghi chú dữ liệu (quan trọng):**
> - **Nguồn BĐS:** Scrapy hit **JSON API chotot** → Kafka. Dataset là **đầu vào thay được (pluggable)**: `chotot_bds_data.csv` hiện là **draft**; dữ liệu thật tích lũy sau khi pipeline xong.
> - **Phạm vi:** chỉ **TP.HCM**.
> - **Vĩ mô ≠ giá nhà.** Là chỉ số nền kinh tế: **giá vàng, tỷ giá USD/VND, VN-Index** (nhịp ngày) + CPI/lãi suất (nhịp tháng, bối cảnh). Ghép theo **`Ngay dang`**.
> - ⚠️ **Hạn chế cửa sổ thời gian:** draft chỉ trải ~7 tuần (2026-07-29 → 09-17) → vĩ mô nhịp tháng gần như hằng số, tín hiệu yếu. Chỉ chuỗi **nhịp ngày** có tín hiệu. Cửa sổ giãn khi có dữ liệu thật tích lũy dài hơn.
>
> **Ghi chú yêu cầu môn học (quan trọng):** Đồ án **không dùng NoSQL** (chỉ PostgreSQL). Yêu cầu mục I.1 cho phép chọn "phần mềm trong hệ sinh thái Dữ liệu lớn **HOẶC** hệ thống thời gian thực"; mục III ghi NoSQL "(nếu có)". → **Đối tượng nghiên cứu chính là Apache Kafka + Apache Spark** (đều là phần mềm hệ sinh thái Big Data). Báo cáo **bắt buộc** trình bày sâu Kafka + Spark để thỏa tiêu chí, nếu không sẽ hụt yêu cầu.

## Cách dùng tài liệu này (cho Claude & nhóm)

- Mỗi milestone là 1 đơn vị **độc lập, tự chứa** — làm được mà không cần đọc lại toàn bộ ngữ cảnh.
- Trước khi làm: đọc **Bảng trạng thái** để biết milestone kế tiếp.
- Sau khi xong 1 milestone: cập nhật cột `Trạng thái` + điền **Checkpoint** (Đã làm gì / Sản phẩm ở đâu / Verify chưa / Ghi chú lần sau).
- Ký hiệu: `TODO` chưa làm · `WIP` đang làm · `DONE` xong & đã verify · `BLOCKED` kẹt.

---

## Hai luồng vận hành (cốt lõi thiết kế)

Hệ thống tách rõ **huấn luyện** và **suy luận** — model KHÔNG train lại mỗi lần crawl.

```text
LUỒNG A — HUẤN LUYỆN (offline, định kỳ / thủ công)
  Dataset tích lũy (CSV + tin đã crawl)
    → Spark batch: ETL → feature → train + tuning (CrossValidator)
    → lưu MODEL có version (vd model/v2026-09-16/)

LUỒNG B — SUY LUẬN (mỗi lần crawl, thường xuyên)
  Tin mới crawl → Kafka → Spark batch
    → NẠP model đã lưu (không train lại)
    → predict giá → tính undervalued_ratio
    → ghi PostgreSQL
```

- **Train (Luồng A):** chạy trên dữ liệu tích lũy, lịch định kỳ (vd hàng tuần) để bắt model drift. Tốn tài nguyên (tuning) → không chạy mỗi crawl.
- **Serve (Luồng B):** mỗi crawl chỉ **load + predict**. Nhanh, rẻ.
- **Theo dõi drift:** mỗi lô mới so RMSE với baseline; lệch nhiều → kích hoạt retrain (Luồng A).

---

## Bảng trạng thái (đọc phần này đầu tiên)

| ID | Milestone | Luồng | Phụ thuộc | Trạng thái | Sản phẩm chính |
|----|-----------|-------|-----------|-----------|----------------|
| M0 | Hạ tầng Docker cluster | — | — | `TODO` | `docker-compose.yml` chạy được |
| M1 | Bài toán + schema + chọn dataset | — | — | `TODO` | `docs/problem_and_schema.md` + dataset |
| M2 | Kafka producer (replay) + scraper nhỏ | A/B | M0, M1 | `TODO` | `scraper/`, producer đẩy topic |
| M3 | Kafka topics + kiểm thử luồng | — | M0 | `TODO` | Topic tạo xong, consume được |
| M4 | Raw landing (Parquet data lake) | — | M2, M3 | `TODO` | `listings_raw/*.parquet` |
| M5 | ETL làm sạch (Spark batch SQL) | — | M3, M4 | `DONE` | `listings_clean/sale/*.parquet` (2,156 dòng) |
| M6 | Feature pipeline + vĩ mô (dùng chung) | A/B | M5 | `TODO` | Spark ML `Pipeline` đã lưu |
| M7 | **Luồng A** — Train + tuning + lưu model | A | M6 | `DONE` | `models/v2026-09-17/` (RandomForest, RMSE 15.13, R² 0.37) |
| M8 | **Luồng B** — Score tin mới (load model) | B | M6, M7 | `DONE` | `data/lake/predictions/sale` (317 tin định giá thấp, drift OK) |
| M9 | Ghi PostgreSQL + trực quan hóa | B | M8 | `TODO` | Bảng `predictions` + dashboard |
| M10 | Báo cáo, slide, video demo | — | M0–M9 | `TODO` | File nộp |

> Cập nhật cột `Trạng thái` mỗi khi hoàn thành. Đây là "trí nhớ" của đồ án.

---

## M0 — Hạ tầng Docker cluster

**Mục tiêu:** một lệnh dựng toàn bộ môi trường, tái lập cho video cài đặt.

**Việc cần làm:**
- [ ] `docker-compose.yml` gồm: `zookeeper`, `kafka`, `spark-master`, `spark-worker` (×2 để thể hiện xử lý song song), `postgres`.
- [ ] Volume dùng chung để Spark đọc/ghi Parquet (thư mục `data/lake/`).
- [ ] Script nạp dataset mẫu vào volume.
- [ ] Cấu trúc thư mục:
  ```text
  project/
  ├── docker-compose.yml
  ├── scraper/      # scraper nhỏ + Kafka producer
  ├── streaming/    # producer replay dataset
  ├── ml/           # train (Luồng A) + score (Luồng B)
  ├── sql/          # DDL PostgreSQL, view báo cáo
  ├── data/
  │   ├── raw_csv/  # dataset gốc
  │   └── lake/     # parquet staging
  ├── models/       # model có version
  ├── notebooks/    # EDA, biểu đồ
  └── docs/         # báo cáo, slide
  ```
- [ ] `git init` + `.gitignore` (bỏ `data/lake/`, `models/*` binary lớn).

**Tiêu chí hoàn thành (verify):** `docker compose up` chạy; `docker compose ps` mọi service `up`; Spark UI (master:8080) thấy 2 worker.

**Checkpoint:**
- Đã làm: _..._ · Sản phẩm ở: _..._ · Verify: _..._ · Ghi chú lần sau: _..._

---

## M1 — Bài toán + schema + chọn dataset

*(Yêu cầu III: "Phát biểu bài toán, mô tả dữ liệu")*

**Mục tiêu:** chốt bài toán, biến mục tiêu, schema, và dataset nguồn.

**Việc cần làm:**
- [ ] Phát biểu bài toán: dự đoán **giá trị thực** BĐS (kết hợp vĩ mô) → so giá rao → tìm BĐS **bị định giá thấp**.
- [ ] Lập luận 3V (Volume/Velocity/Variety).
- [ ] **Dataset BĐS = pluggable.** Draft: `chotot_bds_data.csv`. Cột thật (chotot):

| Cột CSV | Chuẩn hóa → | Ghi chú |
|---|---|---|
| `STT` | `id` | |
| `Tieu de` | `title` | |
| `Gia (VND)` | `price` | **trộn bán + cho thuê → phải tách** (M5) |
| `Dien tich` | `area` (m²) | parse "50 m²" → 50 |
| `Dia chi`, `Quan/Huyen`, `Thanh pho` | `address`, `district`, `city` | chỉ HCM |
| `Loai BDS` | `property_type` | Căn hộ/Chung cư… |
| `Phong ngu`, `So tang`, `Huong` | `bedrooms`, `floors`, `direction` | có null |
| `Ngay dang` | `posted_at` | **khóa ghép vĩ mô**; draft 2026-07-29 → 09-17 |
| `URL` | `url` | |
| `Thoi gian crawl` | `scraped_at` | |

- [ ] Chốt biến mục tiêu: `price_per_m2` (khuyến nghị — bớt lệch theo diện tích).
- [ ] **Chuỗi vĩ mô** (nhịp ngày, ghép theo `posted_at`): giá vàng, USD/VND, VN-Index. Nguồn: xem M6.

**Tiêu chí hoàn thành:** `docs/problem_and_schema.md` đủ phát biểu + bảng schema + biến mục tiêu + danh sách chuỗi vĩ mô.

**Checkpoint:**
- Biến mục tiêu: _..._ · Khoảng thời gian dataset thật: _..._ · Chuỗi vĩ mô chốt: _..._ · Ghi chú lần sau: _..._

---

## M2 — Scraper (Scrapy → chotot API) + producer replay

**Mục tiêu:** đưa tin BĐS vào Kafka. Scraper Scrapy = đường dữ liệu thật; replay CSV = demo/khi chưa cào.

**Việc cần làm:**
- [ ] **Scraper Scrapy — hit JSON API chotot (KHÔNG parse HTML).** Chotot là SPA render JS → cào HTML rỗng. Dùng API công khai kiểu `gateway.chotot.com/v1/public/ad-listing?cg=<nhà đất>&region_v2=<HCM>&...`.
  - **Verify endpoint:** DevTools → Network → lọc `Fetch/XHR` khi duyệt chotot → copy request + params thật.
  - Parse JSON → chuẩn hóa về schema M1 (giá→VND, "50 m²"→50, tách quận, `Ngay dang`).
  - `AutoThrottle` + `DOWNLOAD_DELAY` chống bot; đọc `robots.txt` + ToS chotot (ghi báo cáo — đạo đức/pháp lý).
  - Item Pipeline → **đẩy thẳng Kafka** topic `real_estate_raw`.
  - Fallback nếu API khóa: `scrapy-playwright` render JS.
  - Scheduler cào định kỳ → tích lũy chiều thời gian (dữ liệu thật).
- [ ] **Producer replay (demo):** đọc `chotot_bds_data.csv` → gửi từng dòng JSON vào cùng topic (dùng khi chưa cào / test pipeline).
- [ ] Cả hai đẩy **cùng schema** vào cùng topic.

**Tiêu chí hoàn thành:** message chảy vào Kafka từ scraper (API thật) và replay; đếm khớp số dòng.

**Checkpoint:**
- Endpoint API chotot đã verify: _..._ · Số tin cào thật: _..._ · Vấn đề anti-bot/ToS: _..._ · Ghi chú lần sau: _..._

---

## M3 — Kafka topics + kiểm thử luồng

**Mục tiêu:** lớp đệm streaming ổn định (dù xử lý sau là batch).

**Việc cần làm:**
- [ ] Tạo topic `real_estate_raw` (partitions vd 3 để minh họa song song).
- [ ] Cấu hình `retention` hợp lý.
- [ ] Kiểm thử `kafka-console-consumer` xác nhận định dạng.

> **Cho báo cáo:** vai trò messaging/decoupling của Kafka; ưu điểm (throughput, replay, buffer); so RabbitMQ. **Đây là 1 trong 2 công cụ Big Data trọng tâm.**

**Tiêu chí hoàn thành:** producer ghi + consumer đọc ổn định.

**Checkpoint:**
- Topic + partitions: _..._ · Ghi chú lần sau: _..._

---

## M4 — Raw landing (Parquet data lake)

**Mục tiêu:** lưu tin thô để tái xử lý & tích lũy cho huấn luyện. Thay cho NoSQL — dùng Parquet trên volume chung.

**Việc cần làm:**
- [ ] Spark (hoặc consumer) đọc `real_estate_raw` → ghi thô ra `data/lake/listings_raw/` dạng **Parquet**, phân vùng theo ngày (`dt=YYYY-MM-DD`).
- [ ] Append mỗi lần crawl → dữ liệu **tích lũy** phục vụ Luồng A.

> **Vì sao Parquet, không NoSQL:** đề bài chốt chỉ PostgreSQL; Parquet cột-lưu hợp Spark, làm staging/data-lake nhẹ, không thêm service. (Yêu cầu NoSQL "(nếu có)" → bỏ hợp lệ, xem ghi chú đầu file.)

**Tiêu chí hoàn thành:** `listings_raw/` có partition theo ngày; Spark đọc lại đếm khớp.

**Checkpoint:**
- Số partition/ngày: _..._ · Tổng số dòng tích lũy: _..._ · Ghi chú lần sau: _..._

---

## M5 — ETL làm sạch (Spark batch SQL)

**Mục tiêu:** dữ liệu sạch cho ML.

**Nguồn train:** `data/raw_csv/data_public.csv` (Kaggle, ~51k tin HCM, 05→09/2025). chotot API chỉ ~2 ngày → để dành luồng serve/live. Cả 2 chuẩn hóa về **1 schema chung** để union sau.

**Việc cần làm:**
- [x] Spark **batch** đọc nguồn thô (hiện: `bds_merged.csv`; sau: `listings_raw` Parquet từ Kafka).
- [x] **Tách bán vs cho thuê.** File gộp `bds_merged.csv` KHÔNG còn cột `type` → tách bằng **ngưỡng xác định** (không model): giữ `price ≥ 500 triệu` VÀ `price_per_m2 ≥ 5 triệu/m²`. Rent contamination ~40% (Gia/m2 lưỡng đỉnh: thuê ~0.25 vs bán ~39) bị loại.
- [x] Làm sạch (Spark DataFrame): dedup `url`; parse giá + area ("42 m²"→42); `district`/`city`; ép kiểu ("3.0"→double→int); lọc ngoại lai **IQR 1.5×** (`price_per_m2` & `area`); `price_per_m2` (triệu/m²).
- [x] Ghi `data/lake/listings_clean/sale` Parquet.

> **Rule:** biến đổi xác định (đổi đơn vị, số học, lọc ngưỡng) làm bằng code Spark — không dùng model.

**Tiêu chí hoàn thành:** dataset sạch, không null ở `price`/`area`, số dòng hợp lý. ✅

**Checkpoint (2026-09-17, chạy lại trên `bds_merged.csv`):**
- **Nguồn đổi:** `data_public.csv` (Kaggle 38k) đã BỎ khỏi đĩa. Nguồn hiện tại = `bds_merged.csv` (5,460 dòng = 3,060 chotot + 2,400 data_bds). Code: [ml/etl_clean.ipynb](ml/etl_clean.ipynb) (không còn `train.ipynb`).
- **Quyết định làm sạch chốt với user** (null/scope/rent, 2026-09-17):
  - Scope: **chỉ HCM** (bỏ 613 dòng tỉnh khác: Đà Nẵng, Lâm Đồng, Hà Nội, Bình Dương, Cần Thơ, Đồng Nai).
  - Tách bán/thuê: **ngưỡng** `price ≥ 500tr` & `price_per_m2 ≥ 5tr/m²` (file gộp mất cột `type`).
  - Điền khuyết: `So tang→1`, `Phong ngu→trung vị(2)`, `Nha ve sinh→trung vị(2)`, `Noi that→mode(2)`, `Phap ly→mode(6)`.
  - Bỏ cột: `Huong`, `Loai can ho`, `Diem danh gia`, `Tien coc` (>78% null) + `Loai phi`, `Phong tot` (100% null).
- **Bảng đếm:** 5,460 thô → 4,847 HCM → 2,484 **dedup url** → 2,414 sau lọc thuê (−70) → **2,156 sạch** (−258 ngoại lai IQR 1.5×). Ghi Parquet: 2,156 dòng, **0 null** ở price/area/price_per_m2/bedrooms/bathrooms/floors. `price_per_m2` triệu/m²: min 11 · median 52.5 · max 110 (đúng dải BÁN HCM).
- ⚠️ **chotot_bds_data.csv gần như toàn tin trùng:** 2,448 dòng HCM → chỉ ~85 URL thật (1 ad lặp tới 59×). Giá trị thật đến từ `data_bds` (~2,400 tin unique). Dataset nhỏ (2,156) — cân nhắc cào thêm trước M7.
- **rank_quan (tier quận CỨNG 1–5)** thêm ở M5 (user 2026-09-17: quận huyện quyết định giá). Hard-code `TIER_QUAN` theo phân tầng thị trường HCM — KHÔNG suy từ giá dataset → **0 leakage** (thay `loc_rank` fold-wise từng tính hoãn sang M6). Tier: 5=Q1,Q3 · 4=Q4,Q5,Q7,Q10,Phú Nhuận,Bình Thạnh,Tân Bình · 3=Q6,Q11,Gò Vấp,Tân Phú,Thủ Đức · 2=Q8,Q12,Bình Tân · 1=Nhà Bè,Bình Chánh,Hóc Môn,Củ Chi. Chủ quan — sửa dict nếu cần. `tong_tien_ich` đã bỏ. Output = **23 cột** (21 chotot gốc + price_per_m2 + rank_quan).
- **Môi trường:** Spark cần `JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home` (openjdk@17 đã cài qua brew, chưa link PATH).
- ~~**Ghi chú lần sau (M6):** posted_at cũng 2026~~ → **SAI. M6 phát hiện:** 2142/2156 dòng posted **Dec-2025** (data_bds), chỉ 14 dòng 2026 (chotot). Đã re-fetch macro `--start 2025-11-01` (macro_daily 2025-10-13→2026-09-17). As-of join 0 dòng thiếu. Xem checkpoint M6.

---

## M6 — Feature pipeline + vĩ mô (dùng chung A & B)

**Mục tiêu:** MỘT pipeline đặc trưng dùng chung cho cả train và serve (tránh lệch train/serve).

**3 nhóm đặc trưng:**

1. **Đặc trưng căn:** `log(area)`, one-hot `district`/`property_type`, `bedrooms`, `floors`, khoảng cách tới trung tâm (nếu geocode được).
2. **Vĩ mô (point-in-time, nhịp ngày):** giá vàng, USD/VND, VN-Index — **ghép theo ngày của `posted_at`**, dùng giá trị **đã biết tại ngày đăng** (tránh leakage dùng số tương lai). Tùy chọn: lag `lag_1`/`lag_7` ngày.
3. **Thời gian:** `year`, `month`, `quarter`, `dayofweek`.

**Nguồn vĩ mô (tải nhịp ngày):**
| Chuỗi | Lấy bằng |
|---|---|
| Giá vàng | `yfinance` `GC=F`, hoặc `vnstock` (SJC), cafef |
| USD/VND | `yfinance` `VND=X`, Vietcombank |
| VN-Index | `vnstock`, cafef, investing.com |
| (bối cảnh) CPI, lãi suất | GSO, SBV — nhịp tháng, tín hiệu yếu ở cửa sổ ngắn |

**Việc cần làm:**
- [x] Script tải 3 chuỗi vĩ mô daily → lưu `data/raw_csv/macro/`. → `ml/fetch_macro.py`.
- [x] Job ghép vĩ mô vào `listings_clean` theo ngày `posted_at` (as-of join, không lấy tương lai). → `ml/feature_pipeline.ipynb` cell 3.
- [x] Spark ML `Pipeline`: `SQLTransformer` (cột phái sinh) → `StringIndexer` → `OneHotEncoder` → `VectorAssembler` → `StandardScaler` — **dùng chung train & serve**.
- [x] **Lưu pipeline** (`pipeline.save`) → `models/feature_pipeline` (UNFITTED recipe).


**Tiêu chí hoàn thành:** pipeline fit được, đã lưu; vĩ mô ghép đúng theo ngày, không leakage. ✅

**Checkpoint (M6 XONG 2026-09-17):**
- ✅ **Vĩ mô:** `ml/fetch_macro.py` (venv `.venv`; pandas, yfinance, vnstock). gold `GC=F`, USD/VND `VND=X`, VN-Index (vnstock/VCI). **RE-FETCH `--start 2025-11-01`** vì phát hiện data 98% posted Dec-2025 (không phải 2026 như ghi chú cũ). `macro_daily.csv` giờ 2025-10-13→2026-09-17, 340 ngày ffill (NaN chỉ 21 ngày đầu <2025-11-03, ngoài cửa sổ data).
- ✅ **As-of join:** `ml/feature_pipeline.ipynb`. `posted_date = to_date(Ngay dang)` equi-join `macro_daily` (đã ffill lịch ngày = as-of value đã biết tại ngày đăng, 0 future). **2156 dòng, 0 dòng thiếu macro** (posted 2025-12-10→2026-09-17). Ghi `data/lake/listings_features/sale/` (27 cột).
- ✅ **Pipeline (unfitted recipe)** `models/feature_pipeline/` (5 stages, reload OK). **feature dim = 18** (13 numeric + one-hot `Loai BDS`).
  - Numeric: `log_area, Phong ngu, Nha ve sinh, So tang, rank_quan, dist_center` (haversine tới tâm HCM 10.7769/106.7009), `gold_usd, usdvnd, vnindex`, `year, month, quarter, dayofweek`.
  - Categorical: `Loai BDS` one-hot `handleInvalid=keep` (**98% null** — data_bds thiếu; sẵn cho chotot tương lai).
  - **Quận = `rank_quan` ordinal** (user chốt), KHÔNG one-hot 21 quận → tránh cộng tuyến.
- ⚠️ **Data reality (đọc trước M7):** 2142/2156 dòng dồn Dec-2025 (~11 ngày 10–23/12) + 14 dòng chotot Aug–Sep 2026. Macro gần phẳng trong cụm Dec → **ablation vĩ mô M7 nhiều khả năng ~0 cải thiện** (đúng data, báo cáo trung thực). `Loai BDS`/`Thoi gian crawl` cũng chỉ có ở 14 dòng chotot. Cân nhắc cào thêm tin 2026 đa dạng thời gian trước khi kết luận về vĩ mô.
- ⚠️ **M6 lưu UNFITTED recipe** → M7 phải `pipeline.fit(train_split)` theo thời gian rồi LƯU `PipelineModel` đã fit (StringIndexer labels + Scaler stats đóng băng) cho M8 serve. KHÔNG fit trên toàn bộ (leakage).
- Chạy lại: `JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home .venv/bin/jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.kernel_name=python3 ml/feature_pipeline.ipynb`

---

## M7 — Luồng A: Train + tuning + lưu model

*(Yêu cầu III: "cách điều chỉnh các tham số". Chạy ĐỊNH KỲ, không mỗi crawl.)*

**Mục tiêu:** mô hình hồi quy dự đoán giá, có version.

**Việc cần làm:**
- [x] Đọc `listings_features/sale` (M6, macro đã join) → áp feature stages M6.
- [x] ~~Chia theo thời gian~~ → **random 80/20 (seed=42)** — user chốt 2026-09-17 (xem checkpoint: chia theo năm cho test n=14 vô nghĩa).
- [x] Thử `LinearRegression`, `RandomForestRegressor`, `GBTRegressor`.
- [x] Tuning: `CrossValidator` (3-fold) + `ParamGridBuilder`.
- [x] **Ablation vĩ mô:** train 2 bản — có vs không macro — so RMSE/MAE.
- [x] Đánh giá RMSE/MAE/R² → bảng so sánh (3 model × có/không vĩ mô).
- [x] **Lưu model có version:** `models/v2026-09-17/model` + `metrics.json` (baseline RMSE).

**Tiêu chí hoàn thành:** bảng chỉ số + model versioned đã lưu. ✅

**Checkpoint (M7 XONG 2026-09-17):**
- **Code:** [ml/train.ipynb](ml/train.ipynb) (8 cell). Chạy lại: `JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home .venv/bin/jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.kernel_name=python3 ml/train.ipynb`.
- ⚠️ **Split = random 80/20 (seed=42), KHÔNG chia-theo-thời-gian như spec.** User chốt sau khi thấy data 2142/2156 dồn Dec-2025, chỉ 14 dòng 2026 → chia 2025/2026 cho **test n=14** (vô nghĩa). Random: train 1775 / test 381. Đánh đổi: lệch chủ ý anti-leakage-thời-gian của spec — chấp nhận vì data gần như cùng 1 cửa sổ (~11 ngày) nên leakage thời gian không đáng kể. **Khi có data 2026 đa dạng → đổi lại chia-theo-thời-gian.**
- **Bảng chỉ số (test, target `price_per_m2` triệu/m²):**

  | Model | features | RMSE | MAE | R² |
  |---|---|---|---|---|
  | **RandomForest** | **macro** | **15.13** | 11.11 | **0.366** |
  | GBT | macro | 15.95 | 11.32 | 0.295 |
  | LinearRegression | macro | 17.44 | 13.76 | 0.157 |
  | RandomForest | no_macro | 15.06 | 11.03 | 0.372 |
  | GBT | no_macro | 16.30 | 11.37 | 0.264 |
  | LinearRegression | no_macro | 17.50 | 13.82 | 0.152 |

- **Ablation vĩ mô (Δrmse = macro − no_macro; âm = macro tốt hơn):** RF **+0.07** (macro nhỉnh xấu hơn), GBT −0.35, LR −0.05. **Net ≈ 0** — đúng cảnh báo M6: data dồn ~11 ngày → macro gần hằng số, không cải thiện. Báo cáo trung thực theo số (KHÔNG bịa "vĩ mô cải thiện"). Muốn thấy tín hiệu vĩ mô → cào tin 2026 trải nhiều tháng.
- **Model tốt nhất (nhóm macro, đúng kiến trúc Luồng B):** RandomForest — **RMSE 15.13 · MAE 11.11 · R² 0.366 · baseline_rmse 15.128**.
- **Lưu:** `models/v2026-09-17/model` = **FULL PipelineModel** (feature stages fit trên train + RF). M8 nạp 1 artifact `PipelineModel.load(...)`, `transform` tin mới (đã as-of join macro) → prediction — KHÔNG cần nạp riêng recipe M6. `models/v2026-09-17/metrics.json` = toàn bộ 6 kết quả + baseline (drift check M8).
- **Ghi chú lần sau (M8):** dùng full PipelineModel này; drift check so RMSE lô mới với `baseline_rmse=15.128`. R²=0.37 thấp — dataset nhỏ + đơn điệu thời gian; cải thiện chính = cào thêm data (không phải tune thêm).

---

## M8 — Luồng B: Score tin mới (load model)

**Mục tiêu:** chấm điểm mỗi lần crawl, KHÔNG train lại.

**Việc cần làm:**
- [x] Job Spark batch: đọc tin mới (`listings_clean` lô mới) → **ghép vĩ mô theo `Ngay dang`** (y hệt M6) → **nạp FULL `PipelineModel` M7** (đã gồm feature stages M6 + RF; 1 artifact).
- [x] `predicted_price = model.transform(features)` → cột `predicted_ppm2` (giá/m² triệu).
- [x] `undervalued_ratio = (predicted_ppm2 − listing_ppm2) / predicted_ppm2`.
- [x] Gắn cờ `is_undervalued` khi `ratio ≥ 0.20` (20%, user chốt).
- [x] **Drift check:** RMSE lô mới vs `baseline_rmse` 15.128; cảnh báo "cần retrain" khi `rmse > 1.5×baseline` (>22.69).
- [x] Chạy sau mỗi crawl: đổi `NEW_BATCH` trong cell 1 → chạy lại notebook (cron/thủ công).

**Tiêu chí hoàn thành:** mỗi lô crawl → bảng dự đoán + danh sách top định giá thấp, không train lại. ✅

**Checkpoint (M8 XONG 2026-09-17):**
- **Code:** [ml/score_new.ipynb](ml/score_new.ipynb) (6 cell). Chạy lại: `JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home .venv/bin/jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.kernel_name=python3 ml/score_new.ipynb`.
- **Lô demo:** toàn bộ `data/lake/listings_clean/sale` (2156 tin) — chưa có crawl mới nên tái dùng data hiện có. Production: đổi `NEW_BATCH` → output crawl mới; code không đổi.
- **Ngưỡng:** định giá thấp `ratio ≥ 0.20` → **317 tin (14.7%)**. Top: Q7, Thủ Đức, Bình Chánh (chênh predicted/listing lớn).
- **Trigger retrain:** `rmse > 1.5×baseline` (>22.69) → chạy lại M7 (`ml/train.ipynb`).
- ⚠️ **Drift tính trên HOLDOUT thật (381 dòng test, randomSplit seed=42 y hệt M7), KHÔNG trên full batch** — vì lô demo trùng data đã train → RMSE full lạc quan (11.05). Test-holdout RMSE = **15.128 = baseline** (khớp chính xác) → **OK, không drift**. Khi có crawl mới thật (data lạ) thì full batch chính là holdout, tính RMSE thẳng.
- **Output:** `data/lake/predictions/sale` (2156 dòng): `id, district, area_m2, listing_ppm2, predicted_ppm2, predicted_total_vnd, undervalued_ratio, is_undervalued, URL, model_version, scored_at` → M9 nạp PostgreSQL.
- **Ghi chú lần sau (M9):** predictions parquet đã sẵn; giá lưu theo **triệu/m²** (không phải tổng VND) trừ cột `predicted_total_vnd`. DDL M9 map cột cho khớp đơn vị.

---

## M9 — Ghi PostgreSQL + trực quan hóa

**Mục tiêu:** kết quả cuối theo đúng kiến trúc.

**Việc cần làm:**
- [ ] DDL `predictions(id, listing_price, predicted_price, undervalued_ratio, is_undervalued, district, model_version, scraped_at)` trong `sql/`.
- [ ] Spark ghi PostgreSQL qua JDBC (`format("jdbc")`, `mode append`).
- [ ] View báo cáo: top định giá thấp theo quận, phân bố giá/m², xu hướng theo thời gian.
- [ ] Trực quan: Metabase/Superset hoặc notebook (matplotlib).

**Tiêu chí hoàn thành:** dữ liệu trong PostgreSQL; xuất biểu đồ cho slide.

**Checkpoint:**
- Biểu đồ đã tạo: _..._ · Ghi chú lần sau: _..._

---

## M10 — Báo cáo, slide, video demo

**Mục tiêu:** sản phẩm nộp đủ yêu cầu môn.

**Việc cần làm:**
- [ ] Sơ đồ kiến trúc + **sơ đồ 2 luồng train/serve** (mermaid).
- [ ] Kịch bản demo end-to-end: up cluster → producer/scraper → Kafka → Spark ETL → (train 1 lần) → score lô mới → PostgreSQL/dashboard.
- [ ] Báo cáo (Word/LaTeX) đủ 7 mục phần III — **nhấn Kafka + Spark là công cụ Big Data trọng tâm** (thay cho phần NoSQL).
- [ ] Slide PowerPoint.
- [ ] Video hướng dẫn cài đặt + minh họa.

**Danh mục nộp:**
- [ ] Báo cáo PDF **và** Word/LaTeX
- [ ] File trình chiếu
- [ ] Dữ liệu mẫu + mã nguồn
- [ ] Video cài đặt & minh họa

**Checkpoint:**
- Trạng thái từng sản phẩm: _..._

---

## Bảng ánh xạ: Yêu cầu mục III → Milestone

| Yêu cầu mục III | Milestone |
|---|---|
| Phát biểu bài toán, mô tả dữ liệu | M1 |
| Thông tin chung công cụ Big Data (Kafka, Spark) | M3, M6, M7 |
| Ưu/khuyết điểm, điểm nổi bật | M3, M10 |
| Case study thực tế | M10 |
| Mô hình hệ thống (kiến trúc + 2 luồng) | M10 |
| Cài đặt, kết nối Spark, điều chỉnh tham số | M0, M5, M7 |
| Minh họa xử lý dữ liệu lớn | M4–M9 |
