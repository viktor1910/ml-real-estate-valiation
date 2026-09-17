# Bài toán & Schema dữ liệu

## 1. Phát biểu bài toán

**Bài toán:** Dự đoán giá trị thực của bất động sản tại TP.HCM và phát hiện tin đăng bị **định giá thấp** (undervalued).

**Mục tiêu cụ thể:**
- Huấn luyện mô hình hồi quy dự đoán `price_per_m2` (triệu VND/m²) từ đặc trưng căn hộ + chỉ số vĩ mô tại ngày đăng tin.
- Với mỗi tin mới crawl: so `predicted_price_per_m2` vs `listing_price_per_m2` → tính `undervalued_ratio`.
- Gắn cờ `is_undervalued = True` khi `ratio ≥ 0.20` (giá rao thấp hơn dự đoán ≥ 20%).

**Phạm vi:** chỉ TP.HCM, phân khúc **bán** (loại bỏ thuê).

## 2. Lập luận 3V (Big Data)

| V | Thể hiện |
|---|---|
| **Volume** | Crawl liên tục chotot (hàng nghìn tin/ngày); tích lũy Parquet lake theo thời gian |
| **Velocity** | Kafka làm buffer — scraper đẩy tin mới không đợi Spark xử lý xong |
| **Variety** | Dữ liệu BĐS (text, số, tọa độ) kết hợp chuỗi vĩ mô daily (gold, USD/VND, VN-Index) |

## 3. Kiến trúc hệ thống

```
[Scrapy chotot API]
       ↓ JSON stream
  [Kafka topic: real_estate_raw]  (3 partitions, retention 7 ngày)
       ↓ batch read
  [Spark ETL — M5]  →  data/lake/listings_clean/sale/   (Parquet)
       ↓
  [Spark Feature + Vĩ mô — M6]  →  data/lake/listings_features/sale/
       ↓                                          ↕ as-of join macro_daily.csv
  LUỒNG A: Train (M7)  →  models/v<date>/model   (FULL PipelineModel)
  LUỒNG B: Score (M8)  →  data/lake/predictions/sale/
       ↓
  [PostgreSQL — M9]  →  bảng predictions
       ↓
  [Dashboard / báo cáo — M10]
```

## 4. Schema dữ liệu

### 4a. Raw message Kafka (`real_estate_raw`)

Tất cả cột chuỗi — cast/parse ở ETL M5.

| Cột | Kiểu raw | Nguồn API chotot | Ghi chú |
|---|---|---|---|
| `ad_id` | string | `ad_id` / `list_id` | ID tin, dùng dedup |
| `title` | string | `subject` | Tiêu đề |
| `price` | string | `price` | VND số nguyên (từ API) hoặc số nguyên (từ CSV replay) |
| `price_string` | string | `price_string` | "2.5 tỷ" — chỉ từ API crawl thật |
| `area` | string | `size` / `area_usable` | "60 m²" — parse số ở M5 |
| `ward` | string | `ward_name` | Phường/Xã |
| `district` | string | `area_name` | Quận/Huyện |
| `city` | string | `region_name` | Thành phố |
| `property_type` | string | `category` → map | Loại BĐS |
| `ad_type` | string | `type` | "s"=bán, "r"=thuê — tách ở M5 |
| `bedrooms` | string | `rooms` | Phòng ngủ |
| `bathrooms` | string | `wc` | Nhà vệ sinh |
| `floors` | string | `floors` | Số tầng |
| `direction` | string | `direction` → map | Hướng |
| `interior` | string | `interior` → map | Nội thất |
| `legal` | string | `legal` → map | Pháp lý |
| `apartment_type` | string | `apartment_type` → map | Loại căn hộ |
| `latitude` | double | `latitude` | Tọa độ (null nếu không có) |
| `longitude` | double | `longitude` | Tọa độ (null nếu không có) |
| `posted_at` | string | `orig_list_time` (ms → ISO) | Ngày đăng tin |
| `url` | string | computed | Link tin chotot |
| `crawled_at` | string | computed | ISO datetime lúc crawl |

> ⚠️ **Chưa verify:** field `interior`, `legal`, `apartment_type` từ API chotot có thể là int code hoặc string tùy phiên bản API. Spider log raw value ở lần chạy đầu để xác nhận.

### 4b. Cột bổ sung sau ETL M5 (`listings_clean`)

| Cột | Tính từ |
|---|---|
| `price_per_m2` | `price / area` (triệu VND/m²) |
| `rank_quan` | Hard-code tier quận 1–5 theo thị trường HCM |

### 4c. Output dự đoán M8 (`predictions/sale`)

| Cột | Đơn vị |
|---|---|
| `listing_ppm2` | triệu VND/m² |
| `predicted_ppm2` | triệu VND/m² |
| `predicted_total_vnd` | VND (tổng) |
| `undervalued_ratio` | tỷ lệ thập phân |
| `is_undervalued` | boolean (ratio ≥ 0.20) |

## 5. Biến mục tiêu & chuỗi vĩ mô

**Biến mục tiêu:** `price_per_m2` (triệu VND/m²)  
→ Chuẩn hóa theo diện tích, giảm lệch so với dùng tổng giá.

**Chuỗi vĩ mô (nhịp ngày, ghép theo `posted_at`):**

| Chuỗi | Nguồn | Field |
|---|---|---|
| Giá vàng (USD/oz) | yfinance `GC=F` | `gold_usd` |
| Tỷ giá USD/VND | yfinance `VND=X` | `usdvnd` |
| VN-Index | vnstock/VCI | `vnindex` |

Ghép **as-of join** theo ngày đăng — không dùng giá trị tương lai (tránh leakage).  
Script tải: `ml/fetch_macro.py`.
