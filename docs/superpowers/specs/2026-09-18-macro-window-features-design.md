# Thiết kế: Macro Window Features có Gate + Crawl Macro hằng ngày

- **Ngày:** 2026-09-18
- **Trạng thái:** Đã duyệt thiết kế, chờ viết implementation plan
- **Phạm vi:** ML pipeline dự đoán giá căn hộ HCM (sale). Xử lý biến vĩ mô thành lagged / rolling features, gate theo độ sâu thời gian của dữ liệu.

## 1. Bối cảnh & vấn đề

Pipeline hiện tại (`ml/feature_pipeline.py`) đưa **giá trị vĩ mô thô theo ngày** (`gold_usd`, `usdvnd`, `vnindex`) vào model qua exact-date as-of join. Về lý thuyết ML với cây quyết định (XGBoost/RandomForest), giá trị vĩ mô thô theo ngày là **noise** chứ không phải **signal**: cần lagged features và rolling windows để biểu diễn xu hướng.

**Xung đột với thực tế dữ liệu dự án (đã surface trước khi thiết kế):**

- Nguồn crawl = Chợ Tốt API, cửa sổ **2 ngày**. Pool hiện có **~11–29 dòng HCM sale** dùng được, tất cả chung `posted_at` cách nhau 1–2 ngày.
- Hệ quả: mọi feature vĩ mô (lag, rolling) trở thành **hằng số theo chiều ngang (cross-sectional constant)** trên toàn bộ dòng train → **variance = 0** → cây không có điểm cắt → thêm cột chứ không thêm signal. Đúng ngược mục tiêu.
- Lag/rolling vĩ mô chỉ có ý nghĩa khi listings **trải trên nhiều tháng** posted_at (đủ độ sâu time-series).

**Quyết định:** Xây hạ tầng macro window features theo hướng **Hybrid + Gate** — build đầy đủ, nhưng chỉ bật khi dữ liệu đủ sâu về thời gian; dưới ngưỡng thì drop toàn bộ cột vĩ mô để tránh noise.

## 2. Quyết định đã chốt (qua brainstorming)

| # | Chủ đề | Quyết định |
|---|--------|-----------|
| Q1 | Horizon | **Hybrid**: build infra, gate theo posted_at span |
| Q2 | Nguồn macro | vnstock (daily market) + CPI/lãi suất thủ công (monthly) |
| Q3 | Static CSV | **Bỏ** static CSV. Macro thành nguồn crawl hằng ngày, append vào lake như listings |
| Q4 | Segment sensitivity | **Defer hoàn toàn** — dự án chỉ có căn hộ HCM sale, không có đất nền; interaction term chết ở volume hiện tại. Cây tự học segment qua `property_type` |
| Q5 | Gate threshold | span ≥ **6 tháng** AND ≥ **3 distinct posting months** |
| Q5 | Feature set | Trim theo Rule 2 (chi tiết mục 4) |

## 3. Kiến trúc & thành phần

| Thành phần | File | Vai trò |
|---|---|---|
| Macro fetcher | `ml/fetch_macro.py` *(mới)* | vnstock → market series daily (vnindex/usdvnd/gold); append vào macro lake |
| Macro lake | `data/lake/macro_raw/` *(mới)* | Thay `macro_daily.csv` tĩnh; lịch sử đầy đủ tích lũy, partition `dt=` |
| Monthly macro | `data/raw_csv/macro/cpi_rate_monthly.csv` *(mới)* | CPI + lãi suất, append thủ công hằng tháng (GSO/SBV công bố theo tháng) |
| Window builder | trong `ml/feature_pipeline.py` | Spark trailing-window functions → 14 features |
| Gate | trong `ml/feature_pipeline.py` | Kiểm tra span/tháng → macro ON/OFF, log rõ ràng |

## 4. Bộ feature vĩ mô (khi Gate = ON) — 14 cột

**Market series (daily — vnindex, usdvnd, gold):**
- `vnindex_90d_ma`, `vnindex_90d_pct`
- `usdvnd_90d_ma`, `usdvnd_90d_pct`
- `gold_90d_ma`, `gold_90d_pct`

**Monthly series (CPI, lãi suất):**
- `cpi_1m_lag`, `cpi_3m_lag`, `cpi_6m_lag`, `cpi_90d_pct`
- `rate_1m_lag`, `rate_3m_lag`, `rate_6m_lag`, `rate_90d_pct`

**Bỏ có chủ đích (Rule 2 — cắt noise):**
- **Raw same-day value** của mọi series — nguồn noise chính, đúng như phân tích ban đầu.
- **30-day moving avg** của market — BĐS quá kém thanh khoản để phản ứng chu kỳ 30 ngày của chứng khoán/vàng; noise > signal.
- **CPI/rate value tháng hiện tại** — tránh **data leakage**: GSO/SBV công bố cuối tháng/đầu tháng sau, nên tại thời điểm đăng giữa tháng, CPI tháng đó chưa tồn tại. `1m_lag` là baseline chuẩn nhất mà người mua/bán thực tế tiếp cận được.

## 5. Data flow

```
DAILY  run_daily.sh:
  crawl chotot ─┐
  fetch_macro ──┴─► macro_raw lake (append dt)      [market series]

MONTHLY (thủ công): append cpi_rate_monthly.csv       [CPI, lãi suất]

RETRAIN feature_pipeline.py:
  đọc macro_raw FULL history
   → Spark trailing windows
       (market: 90d MA + 90d pct-change;
        monthly: 1/3/6-mo lag + 90d pct-change)
   → as-of join theo posted_date (leakage-safe: chỉ macro ≤ posted_date)
   → GATE: pool span ≥ 6 tháng AND ≥ 3 distinct months?
        ON  → gắn 14 cột macro, log "MACRO GATE=ON"
        OFF → drop toàn bộ cột macro, log "MACRO GATE=OFF (span=Xd, months=Y)"
   → ghi listings_features/sale
```

## 6. Leakage guard (bắt buộc)

- **Mọi window là trailing-only**: `rangeBetween(unboundedPreceding, currentRow)` kết thúc tại `posted_date`. Không nhìn dòng tương lai.
- **Monthly series**: lag nhỏ nhất = **1 tháng** (không bao giờ dùng tháng hiện tại) → listing đăng 15/05 chỉ thấy CPI ≤ tháng 04. Ràng buộc bằng cấu trúc feature, không phải bằng filter.

## 7. Sửa downstream (surgical — Rule 3)

- `ml/feature_pipeline.py` (dòng ~88–92): danh sách `NUM` thành **động** — phát hiện cột macro nào đang hiện diện (gate có thể drop), không hardcode `gold_usd/usdvnd/vnindex`.
- `ml/score_new.py`: macro join đọc `macro_raw` + tái dùng window builder (không đọc static CSV).
- `ml/train.py`: DERIVE_SQL / feature stages tiêu thụ danh sách cột macro động.

## 8. Error handling

- vnstock fetch fail → **không crash daily loop**; carry-forward dòng macro cuối cùng đã biết, log WARN.
- Thiếu tháng CPI/rate → forward-fill giá trị công bố gần nhất, log.

## 9. Tests (Rule 9 — mã hoá WHY, không chỉ behavior)

- **Gate OFF**: pool synth span 2 ngày → **0 cột macro** (chứng minh hằng số bị loại, không chỉ "chạy được").
- **Gate ON**: pool synth span 7 tháng, 4 distinct months → **đúng 14 cột macro**.
- **Leakage**: listing đăng 15/05 → `cpi_1m_lag` == giá trị **tháng 04**, KHÔNG phải tháng 05.
- **Trailing window**: `vnindex_90d_ma` tại ngày D bỏ qua D+1 (không rò dữ liệu tương lai).

## 10. Rủi ro / giả định cần verify khi implement

- **Coverage vnstock** cho gold / CPI / lãi suất **chưa xác minh** (Rule 1). Spike-check API vnstock khi implement; nếu thiếu series nào → degrade sang manual CSV cho series đó, **không đổi thiết kế**.
- CPI/lãi suất là dữ liệu **tháng** — "daily crawl" thực chất = daily market series + monthly macro carry-forward.

## 11. Ngoài phạm vi (defer)

- Segment × macro interaction (Q4).
- Kafka producer/consumer thật (over-engineering ở volume hiện tại; lake append là đủ).
- Thêm series vĩ mô ngoài 5 series đã chốt.
