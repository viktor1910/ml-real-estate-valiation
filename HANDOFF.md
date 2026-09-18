# HANDOFF — Real Estate Valuation (đọc đầu tiên khi mở session mới)

> Mục tiêu session tới: **xử lý dữ liệu + train model** trên data hiện có.
> Plan đầy đủ + checkpoint: [cac_buoc_thuc_hien_do_an.md](cac_buoc_thuc_hien_do_an.md). File này chỉ tóm trạng thái.

## Trạng thái hiện tại (2026-09-17)

| Milestone | Trạng thái |
|---|---|
| Thiết kế plan (M0–M10) | ✅ chốt |
| M2 — crawler chotot (Scrapy JSON API) | ✅ xong (`scraper/crawl_chotot.py`) — cho luồng serve/live |
| M6 — script tải vĩ mô | ✅ xong (`ml/fetch_macro.py`) — ⚠ phải chạy lại cho cửa sổ 2025 |
| M5 — ETL clean | ✅ **xong** (`train.ipynb`) → `data/lake/listings_clean/sale` (49,159 dòng) |
| M6 — join vĩ mô + Spark ML Pipeline | ⏳ **bắt đầu ở đây** (cần re-fetch macro 2025 trước) |
| M7 — train + tuning + ablation | ⏳ chưa |
| M0–M4, M8–M10 | ⏳ chưa (Kafka/Docker/Postgres để sau) |

## Có sẵn trên đĩa

- **`data/raw_csv/data_public.csv`** — **nguồn train chính** (Kaggle, 51,304 tin BĐS HCM, toàn tin BÁN, `Last Updated Date` 05→09/2025, 16 ngày). Giá cột `Price` đơn vị **triệu VND**. Địa chỉ trộn cấu trúc cũ (Quận) + mới 2025 (chỉ Phường).
- **`data/lake/listings_clean/sale`** — **Parquet sạch từ M5** (49,159 dòng, schema chung, `price` VND, `price_per_m2`, `loc_key`, `posted_at`). Đầu vào M6.
- `scraper/crawl_chotot.py` — crawler chotot gateway JSON API (dedup `list_id`, cột `type` s/u). Cho luồng **serve/live**, chạy: `python scraper/crawl_chotot.py --target N`.
- `data/raw_csv/chotot_listings.csv` — 4,000 tin chotot thật (1 ngày) từ crawler. Dùng test luồng live, KHÔNG train (thiếu chiều thời gian).
- `data/raw_csv/chotot_bds_data.csv` — draft cũ; **thực chất chỉ 70 tin unique** bị crawl lặp ~30× (dùng làm bằng chứng bug dedup ở M2).
- `data/raw_csv/macro/macro_daily.csv` — vĩ mô nhịp ngày. ⚠ **đang là cửa sổ 2026** — chạy lại `python ml/fetch_macro.py --start 2025-05-01 --end 2025-10-01` cho khớp data_public.
- `.venv/` — python 3.14: `pandas`, `yfinance`, `vnstock`, **`pyspark 4.2.0`**, **`scrapy 2.19`**, `ipykernel`, `nbconvert`.
- **Java:** OpenJDK 17 qua brew. Chạy Spark phải set `export JAVA_HOME=/opt/homebrew/opt/openjdk@17`.

## Resume môi trường

```bash
cd "/Users/viktornguyen/Desktop/viktor/Machine learning"
source .venv/bin/activate
```

## Quyết định đã chốt (KHÔNG bàn lại — xem plan để biết lý do)

1. **Mục tiêu:** định giá từng căn (`price_per_m2`) → tìm undervalued. Không phải pure trend forecast.
2. **Vĩ mô** = gold_usd, usdvnd, vnindex (nhịp ngày), **as-of join theo `Ngay dang`** (không leakage).
3. **Tách bán vs cho thuê** trong ETL (dataset trộn: tin tỷ=bán, tin triệu=thuê). Chỉ train trên **bán**.
4. **Split theo thời gian** (train tháng cũ, test tháng mới), **KHÔNG random**.
5. **Ablation vĩ mô** (có/không) để chứng minh giá trị vĩ mô — bắt buộc cho báo cáo.
6. Dataset = **pluggable**, draft nhỏ (~7 tuần) → vĩ mô tín hiệu yếu, chấp nhận cho draft.
7. Chỉ **PostgreSQL** (không NoSQL); Kafka+Spark = công cụ Big Data trọng tâm.

## Việc session tới (thứ tự)

**M5 — ETL clean** ✅ **XONG** → `data/lake/listings_clean/sale` (49,159 dòng). Code trong [train.ipynb](train.ipynb).

**M6 — features (BẮT ĐẦU Ở ĐÂY):**
- **Trước tiên:** re-fetch macro 2025: `export JAVA_HOME=/opt/homebrew/opt/openjdk@17; python ml/fetch_macro.py --start 2025-05-01 --end 2025-10-01`.
- As-of join `macro_daily.csv` theo `posted_at` (date).
- 3 nhóm feature: căn (log area, one-hot quận/loại, phòng) / vĩ mô / thời gian (year/month/quarter).
- Pipeline dùng chung train+serve, lưu lại.

**M7 — train:**
- Split theo thời gian. Thử LinearRegression / RandomForest / GBT.
- Tuning CrossValidator. Ablation có/không vĩ mô. Bảng RMSE/MAE/R². Lưu model versioned.

## Lưu ý kỹ thuật

- Draft nhỏ (~1160 dòng, lọc bán còn ít hơn) → **Spark là overkill về hiệu năng** nhưng cần cho đúng đề bài (công cụ Big Data). Có thể prototype logic bằng pandas trước rồi port sang Spark, HOẶC làm thẳng PySpark local. Quyết ở session tới.
- vnstock in quảng cáo ra stdout khi import — vô hại.
- Chưa phải git repo. `.venv/`, `data/` sẽ gitignore khi làm M0.
