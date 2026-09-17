#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
etl_clean

Auto-converted from etl_clean.ipynb for deployment.
Spark job: requires JAVA_HOME set to a JDK 17 install, e.g.
    export JAVA_HOME=/opt/homebrew/opt/openjdk@17
Run:
    python3 etl_clean.py
"""


# # M5 — ETL làm sạch (Spark batch)
#
# Đọc `data/raw_csv/bds_merged.csv` → lọc, làm sạch, ghi Parquet `data/lake/listings_clean/sale/`.
#
# **Giữ nguyên schema chotot** (bds_merged là chuẩn — chotot crawl sau này theo đúng format này). KHÔNG dịch tên cột sang English. Chỉ:
# - parse kiểu tại chỗ (giá/diện tích/số phòng/ngày),
# - drop cột thừa,
# - thêm 2 cột phái sinh: `price_per_m2` (target) + `rank_quan` (tier quận cứng).
#
# **Quyết định làm sạch (chốt với user 2026-09-17):**
# - **Phạm vi:** CHỈ TP.HCM (bỏ ~613 dòng tỉnh khác).
# - **Tách bán/thuê:** ngưỡng xác định — giữ `Gia (VND) ≥ 500 triệu` VÀ `price_per_m2 ≥ 5 triệu/m²`.
# - **Ngoại lai:** IQR 1.5× trên `price_per_m2` & `Dien tich`.
# - **Điền khuyết:** `So tang`→1 · `Phong ngu`/`Nha ve sinh`→trung vị · `Noi that`/`Phap ly`→mode.
# - **Bỏ cột:** Huong, Loai can ho, Diem danh gia, Tien coc, Loai phi, Phong tot.
# - **rank_quan:** tier quận CỨNG 1–5 (hard-code `TIER_QUAN`), không suy từ giá dataset → 0 leakage. Thay cho `loc_rank` fold-wise từng cân nhắc ở M6.

# ## 1. Cấu hình + Java

import os

# chạy được từ project root HOẶC từ ml/ (notebook CWD)
if os.path.basename(os.getcwd()) == "ml":
    os.chdir("..")

# Spark 4.2 cần Java 17 — openjdk@17 cài qua brew nhưng chưa link PATH
os.environ.setdefault(
    "JAVA_HOME",
    "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home",
)

from pyspark.sql import SparkSession, functions as F

SRC = "data/raw_csv/bds_merged.csv"
OUT = "data/lake/listings_clean/sale"

MIN_PRICE_VND = 500_000_000       # >= 500 triệu
MIN_PPM_TRIEU = 5.0               # >= 5 triệu/m²

DROP_COLS = ["Huong", "Loai can ho", "Diem danh gia", "Tien coc", "Loai phi", "Phong tot"]

# rank_quan: tier thị trường CỨNG (1=rẻ nhất … 5=đắt nhất). Hard-code theo phân
# tầng hành chính/thị trường HCM — KHÔNG suy từ giá dataset => 0 leakage khi M6
# chia fold. Chủ quan, sửa trực tiếp dict này nếu muốn. Quận lạ -> null (in cảnh báo).
TIER_QUAN = {
    "Quận 1": 5, "Quận 3": 5,
    "Quận 4": 4, "Quận 5": 4, "Quận 7": 4, "Quận 10": 4,
    "Quận Phú Nhuận": 4, "Quận Bình Thạnh": 4, "Quận Tân Bình": 4,
    "Quận 6": 3, "Quận 11": 3, "Quận Gò Vấp": 3, "Quận Tân Phú": 3,
    "Thành phố Thủ Đức": 3,
    "Quận 8": 2, "Quận 12": 2, "Quận Bình Tân": 2,
    "Huyện Nhà Bè": 1, "Huyện Bình Chánh": 1, "Huyện Hóc Môn": 1, "Huyện Củ Chi": 1,
}

# ## 2. Helper parse (chịu float-string '3.0', rỗng → null)

def _num(col):
    """chuỗi -> double, chịu float-string ('3.0'), rỗng -> null."""
    x = F.trim(F.col(col))
    x = F.when((x == "") | (F.lower(x).isin("nan", "none", "null", "[]")), None).otherwise(x)
    return x.cast("double")


def _int(col):
    return _num(col).cast("int")

# ## 3. Khởi tạo Spark + đọc CSV thô

spark = (
    SparkSession.builder.appName("M5-etl-clean")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

raw = spark.read.option("header", True).option("multiLine", True).csv(SRC)
n_raw = raw.count()
print("thô:", n_raw, "| cột:", len(raw.columns))

# ## 4. Parse kiểu tại chỗ (giữ tên cột chotot) + drop cột thừa
#
# Ép string thô → số/ngày nhưng **giữ nguyên tên cột chotot**. `Dien tich` "42 m²" → 42.0.

df = (
    raw
    .withColumn("Gia (VND)", _num("Gia (VND)"))
    .withColumn("Gia/m2 (trieu)", _num("Gia/m2 (trieu)"))
    # "42 m²" -> 42.0
    .withColumn("Dien tich",
                F.regexp_extract(F.col("Dien tich"), r"([0-9]+(?:\.[0-9]+)?)", 1).cast("double"))
    .withColumn("Vi do", _num("Vi do"))
    .withColumn("Kinh do", _num("Kinh do"))
    .withColumn("Phong ngu", _int("Phong ngu"))
    .withColumn("Nha ve sinh", _int("Nha ve sinh"))
    .withColumn("So tang", _int("So tang"))
    .withColumn("Noi that", _int("Noi that"))       # enum code
    .withColumn("Phap ly", _int("Phap ly"))         # enum code
    .withColumn("Ngay dang", F.to_timestamp(F.col("Ngay dang")))
    .drop(*DROP_COLS)
)
print("còn lại cột:", len(df.columns))
df.printSchema()

# ## 5. Lọc scope HCM · bỏ giá/area ≤0 · dedup URL

df = df.filter(F.col("Thanh pho").contains("Hồ Chí Minh"))
n_hcm = df.count()

df = df.filter((F.col("Gia (VND)") > 0) & (F.col("Dien tich") > 0))
n_valid = df.count()

df = df.dropDuplicates(["URL"])
n_dedup = df.count()
print(f"HCM {n_hcm} -> valid {n_valid} -> dedup {n_dedup}")

# ## 6. Target `price_per_m2` + tách bán/thuê bằng ngưỡng

df = df.withColumn("price_per_m2", (F.col("Gia (VND)") / 1_000_000) / F.col("Dien tich"))  # triệu/m²
df = df.filter((F.col("Gia (VND)") >= MIN_PRICE_VND) & (F.col("price_per_m2") >= MIN_PPM_TRIEU))
n_sale = df.count()
print("sau lọc thuê:", n_sale)

# ## 7. Lọc ngoại lai IQR 1.5× (`price_per_m2` & `Dien tich`)

for col in ("price_per_m2", "Dien tich"):
    q1, q3 = df.approxQuantile(col, [0.25, 0.75], 0.0)
    iqr = q3 - q1
    lo, hi = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    df = df.filter((F.col(col) >= lo) & (F.col(col) <= hi))
n_clean = df.count()
print("sau IQR:", n_clean)

# ## 8. Điền khuyết (So tang=1 · Phong ngu/Nha ve sinh=trung vị · Noi that/Phap ly=mode)

df = df.fillna({"So tang": 1})
med = df.approxQuantile(["Phong ngu", "Nha ve sinh"], [0.5], 0.0)
med_bed, med_bath = int(med[0][0]), int(med[1][0])
df = df.fillna({"Phong ngu": med_bed, "Nha ve sinh": med_bath})


def _mode(col):
    r = (df.filter(F.col(col).isNotNull())
           .groupBy(col).count().orderBy(F.desc("count")).first())
    return r[0] if r else None


mode_furn, mode_legal = _mode("Noi that"), _mode("Phap ly")
fill = {k: v for k, v in {"Noi that": mode_furn, "Phap ly": mode_legal}.items() if v is not None}
if fill:
    df = df.fillna(fill)
print(f"điền: So tang=1, Phong ngu={med_bed}, Nha ve sinh={med_bath}, Noi that={mode_furn}, Phap ly={mode_legal}")

# ## 8b. `rank_quan` — tier quận cứng (1=rẻ … 5=đắt, không leakage)

from itertools import chain

# dict -> Spark map column; quận không có trong bảng -> null
tier_map = F.create_map([F.lit(x) for x in chain.from_iterable(TIER_QUAN.items())])
df = df.withColumn("rank_quan", tier_map[F.col("Quan/Huyen")].cast("int"))

# cảnh báo nếu có quận chưa map (dữ liệu chotot tương lai có thể thêm quận mới)
n_unmapped = df.filter(F.col("rank_quan").isNull()).count()
if n_unmapped:
    miss = [r[0] for r in df.filter(F.col("rank_quan").isNull())
            .select("Quan/Huyen").distinct().collect()]
    print(f"⚠ {n_unmapped} dòng quận CHƯA có tier -> thêm vào TIER_QUAN: {miss}")
else:
    print("rank_quan: tất cả quận đã map, 0 null")

df.groupBy("rank_quan").count().orderBy("rank_quan").show()

# ## 9. Ghi Parquet

df.write.mode("overwrite").parquet(OUT)
print("ghi ->", OUT)

# ## 10. Verify — đếm dòng, null, phân bố target

back = spark.read.parquet(OUT)
n_out = back.count()
key_cols = ["Gia (VND)", "Dien tich", "price_per_m2", "Phong ngu", "Nha ve sinh", "So tang"]
nulls = back.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in key_cols]).first().asDict()

print("=" * 56)
print("M5 ETL — bảng đếm dòng")
print("=" * 56)
print(f"  thô (bds_merged)          : {n_raw:>6}")
print(f"  sau lọc HCM               : {n_hcm:>6}  (-{n_raw - n_hcm} tỉnh khác)")
print(f"  sau bỏ price/area<=0       : {n_valid:>6}  (-{n_hcm - n_valid})")
print(f"  sau dedup URL             : {n_dedup:>6}  (-{n_valid - n_dedup})")
print(f"  sau lọc thuê (ngưỡng)     : {n_sale:>6}  (-{n_dedup - n_sale} tin thuê)")
print(f"  sau IQR ngoại lai         : {n_clean:>6}  (-{n_sale - n_clean})")
print(f"  ghi Parquet               : {n_out:>6}")
print("-" * 56)
print(f"  null ở cột khóa           : {nulls}")
print(f"  cột output ({len(back.columns)}): {back.columns}")
back.select("price_per_m2").summary("min", "25%", "50%", "75%", "max").show()

spark.stop()
