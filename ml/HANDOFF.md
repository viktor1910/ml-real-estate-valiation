# ML Handoff — Phần lõi ML (M5–M8) để tích hợp

> Tài liệu này dành cho thành viên **tích hợp** phần ML vào hệ thống: người làm
> **M9 (PostgreSQL + dashboard)** đọc output, và người làm **M0–M4 (Docker/Kafka/data lake)**
> cấp input cho luồng scoring. Đọc file này là đủ để nối dây, không cần đọc lại toàn bộ code.
>
> Kế hoạch tổng thể + rationale từng quyết định: [../cac_buoc_thuc_hien_do_an.md](../cac_buoc_thuc_hien_do_an.md).

---

## 1. TL;DR — lõi ML đã xong gì

| Milestone | Trạng thái | Cho ra |
|---|---|---|
| M5 ETL làm sạch | ✅ DONE | `data/lake/listings_clean/sale` (2,156 tin HCM sạch) |
| M6 Feature + vĩ mô | ✅ DONE | `models/feature_pipeline/` (recipe) + `data/lake/listings_features/sale` |
| M7 Train (Luồng A) | ✅ DONE | `models/v2026-09-17/model` (RandomForest, RMSE 15.13, R² 0.37) + `metrics.json` |
| M8 Score (Luồng B) | ✅ DONE | `data/lake/predictions/sale` (2,156 dòng, 317 tin định giá thấp) |

**Toàn bộ lõi ML là notebook Spark chạy được ngay** (không cần Kafka/Postgres để test). Điểm nối:
- **Đầu vào Luồng B** = Parquet `listings_clean/sale` (M2–M4 sẽ thay bằng crawl thật).
- **Đầu ra Luồng B** = Parquet `predictions/sale` (M9 đọc → PostgreSQL).

---

## 2. Kiến trúc 2 luồng (bối cảnh nối dây)

```text
LUỒNG A — TRAIN (offline, định kỳ)     LUỒNG B — SERVE (mỗi crawl)
  listings_features (M6)                 listings_clean lô mới (M4/M5)
    → train + tuning (M7)                  → ghép vĩ mô (as-of join)
    → LƯU PipelineModel có version         → NẠP PipelineModel (KHÔNG train lại)
       models/v<date>/model                → predict + undervalued_ratio + drift check
                                           → GHI predictions/sale  → (M9) PostgreSQL
```

Model **không train lại mỗi crawl**. M8 chỉ `load + transform`. Retrain (chạy lại M7) chỉ khi drift.

---

## 3. Artifacts & đường dẫn (tất cả tương đối project root)

| Artifact | Đường dẫn | Ghi chú |
|---|---|---|
| Data sạch (input serve) | `data/lake/listings_clean/sale/*.parquet` | Output M5 |
| Data + feature/vĩ mô | `data/lake/listings_features/sale/*.parquet` | Output M6 (dùng cho train) |
| Vĩ mô daily | `data/raw_csv/macro/macro_daily.csv` | `date,gold_usd,usdvnd,vnindex` (2025-10-13→2026-09-17, ffill) |
| Feature recipe (unfitted) | `models/feature_pipeline/` | M6; M7 fit rồi gộp vào model dưới |
| **Model production** | `models/v2026-09-17/model/` | **FULL PipelineModel** = feature stages + RF (1 artifact) |
| Metrics + baseline | `models/v2026-09-17/metrics.json` | `baseline_rmse=15.128`, 6 kết quả ablation |
| **Output dự đoán** | `data/lake/predictions/sale/*.parquet` | Bảng M9 nạp Postgres |
| Notebooks | `ml/etl_clean.ipynb` `feature_pipeline.ipynb` `train.ipynb` `score_new.ipynb` | M5/M6/M7/M8 |
| Fetch vĩ mô | `ml/fetch_macro.py` | tải gold/USD-VND/VN-Index |

`models/v2026-09-17/model` là **FULL PipelineModel** — nạp 1 lần là gồm cả feature stages (StringIndexer/OneHot/Scaler đã đóng băng theo train split) + RandomForest. **Không cần nạp riêng recipe M6.**

---

## 4. HỢP ĐỒNG TÍCH HỢP (đọc kỹ)

### 4a. Đầu ra `predictions/sale` — cho người làm M9 (PostgreSQL)

Schema chính xác notebook M8 ghi ra (`ml/score_new.ipynb` cell 5):

| Cột | Kiểu | Đơn vị / ý nghĩa |
|---|---|---|
| `id` | long | STT tin gốc |
| `district` | string | Quận/Huyện (HCM) |
| `area_m2` | double | Diện tích m² |
| `listing_ppm2` | double | **Giá rao / m² (TRIỆU VND)** |
| `predicted_ppm2` | double | **Giá dự đoán / m² (TRIỆU VND)** |
| `predicted_total_vnd` | long | Giá dự đoán tổng = ppm2 × area × 1e6 (**VND**) |
| `undervalued_ratio` | double | `(predicted − listing) / predicted`; dương = rao rẻ hơn dự đoán |
| `is_undervalued` | boolean | `ratio ≥ 0.20` (ngưỡng 20%) |
| `URL` | string | Link tin |
| `model_version` | string | vd `"2026-09-17"` (khớp thư mục model) |
| `scored_at` | string | ISO timestamp lúc chấm |

> ⚠️ **BẪY ĐƠN VỊ:** `listing_ppm2` và `predicted_ppm2` là **triệu VND/m²** (vd 52.5).
> Chỉ `predicted_total_vnd` là VND thô. DDL Postgres phải đặt tên/kiểu cho khớp — đừng nhân/chia nhầm.

**Gợi ý DDL (M9):**
```sql
CREATE TABLE predictions (
  id                   BIGINT,
  district             TEXT,
  area_m2              DOUBLE PRECISION,
  listing_ppm2         DOUBLE PRECISION,   -- triệu/m²
  predicted_ppm2       DOUBLE PRECISION,   -- triệu/m²
  predicted_total_vnd  BIGINT,             -- VND
  undervalued_ratio    DOUBLE PRECISION,
  is_undervalued       BOOLEAN,
  url                  TEXT,
  model_version        TEXT,
  scored_at            TIMESTAMP
);
```
Spark ghi Postgres qua JDBC `format("jdbc").mode("append")`. Có thể đọc thẳng
`predictions/sale` bằng `spark.read.parquet(...)` rồi `.write.jdbc(...)` — không cần đổi cột.

### 4b. Đầu vào Luồng B — cho người làm M0–M4 (Kafka/scraper/data lake)

M8 (`score_new.ipynb` cell 1, biến `NEW_BATCH`) đọc **Parquet** cùng schema với `listings_clean/sale`.
Khi crawl thật hoạt động: chỉ cần ghi lô mới vào một thư mục Parquet rồi trỏ `NEW_BATCH` vào đó — **code M8 không đổi**.

Cột **bắt buộc** trong lô mới (tên giữ nguyên tiếng Việt, không dịch — model fit theo tên này):

| Cột | Dùng để |
|---|---|
| `STT` | id output |
| `Ngay dang` | **khóa ghép vĩ mô** (as-of join theo ngày) |
| `Dien tich` | feature + tính total_vnd |
| `Quan/Huyen` | district |
| `rank_quan` | feature tier quận (1–5), tính ở M5 |
| `Loai BDS`, `Phong ngu`, `Nha ve sinh`, `So tang` | feature |
| `price_per_m2` | giá thực (để tính undervalued + drift) |
| `URL` | output |

> Tin mới **phải đi qua ETL M5** trước (tách bán/thuê, parse giá/area, tính `price_per_m2` + `rank_quan`)
> để ra đúng schema này. Đừng đẩy JSON chotot thô thẳng vào M8.

---

## 5. Chạy lại (môi trường + lệnh)

**Môi trường (một lần):**
- Python venv: `.venv` (project root) — có `pyspark, pandas, yfinance, vnstock`.
- **Java 17 bắt buộc cho Spark:** `openjdk@17` (cài qua brew, chưa link PATH → set `JAVA_HOME` mỗi lần).

**Lệnh chạy notebook (nbconvert, in-place):**
```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home \
  .venv/bin/jupyter nbconvert --to notebook --execute --inplace \
  --ExecutePreprocessor.kernel_name=python3 ml/score_new.ipynb
```
Đổi tên file cho từng bước: `etl_clean.ipynb` → `feature_pipeline.ipynb` → `train.ipynb` → `score_new.ipynb`.
Notebook tự `chdir("..")` nếu chạy trong `ml/` nên đường dẫn tương đối luôn tính từ project root.

**Cập nhật vĩ mô (khi cần cửa sổ dài hơn):**
```bash
.venv/bin/python ml/fetch_macro.py --start 2025-11-01
```

---

## 6. Drift & retrain (vòng đời model)

- M8 tính **RMSE trên holdout thật** (381 dòng test, `randomSplit seed=42` y hệt M7) so với `baseline_rmse=15.128`.
- **Cờ retrain:** `rmse > 1.5 × baseline` (>22.69) → chạy lại M7 (`train.ipynb`) → sinh `models/v<date-mới>/` → đổi `VERSION` trong `score_new.ipynb`.
- Model versioned theo ngày; `model_version` đi kèm mỗi dòng predictions để M9 truy vết.

---

## 7. Hạn chế phải biết (đừng hiểu nhầm số liệu)

- **R² ≈ 0.37, RMSE 15.13 triệu/m²** — dataset nhỏ (2,156) và **dồn ~11 ngày Dec-2025** (2,142/2,156 dòng). Cải thiện chính = **cào thêm data đa dạng thời gian**, không phải tune thêm.
- **Vĩ mô ~0 cải thiện** trong ablation (macro vs no_macro net ≈ 0) — đúng vì data gần như cùng một cửa sổ → vĩ mô gần hằng số. **Báo cáo trung thực theo số, không bịa "vĩ mô cải thiện".**
- **Split hiện tại = random 80/20**, KHÔNG chia-theo-thời-gian (spec gốc) — vì chỉ 14 dòng 2026 thì test theo năm vô nghĩa. Khi có data 2026 trải nhiều tháng → đổi lại chia-theo-thời-gian.
- **Lô demo M8 trùng data train** → RMSE full-batch (11.05) lạc quan; con số dùng để đánh giá là **test-holdout (15.128)**. Khi có crawl thật (data lạ) thì full batch chính là holdout.
- `Loai BDS` **98% null** (chỉ 14 dòng chotot có) — one-hot `handleInvalid=keep`, sẵn cho chotot tương lai.
- Ngưỡng `rank_quan` (tier quận) là **hard-code chủ quan** trong M5 — sửa dict `TIER_QUAN` nếu phân tầng thị trường đổi. Không leakage (không suy từ giá).
