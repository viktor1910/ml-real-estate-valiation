#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""fetch_macro — lấy market series (vnstock) cho hôm nay, append macro_raw lake.

Series: vnindex (VCI close), usdvnd (VCB sell), gold_usd (SJC sell HCM —
tên cột giữ theo schema cũ dù đơn vị là VND/lượng, model dùng như 1 tín
hiệu vĩ mô). Idempotent theo partition dt. vnstock fail -> exit 0 + log
WARN (không làm vỡ daily loop; feature_pipeline carry-forward dòng cũ).
"""
import os
import sys
import datetime

if os.path.basename(os.getcwd()) == "ml":
    os.chdir("..")
os.environ.setdefault(
    "JAVA_HOME", "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home")

LAKE = "data/lake/macro_raw"
TODAY = datetime.date.today().isoformat()
PART = f"{LAKE}/dt={TODAY}"


def _fetch_vnindex() -> float:
    from vnstock import Quote
    start = (datetime.date.today() - datetime.timedelta(days=10)).isoformat()
    h = Quote(symbol="VNINDEX", source="VCI").history(
        start=start, end=TODAY, interval="1D")
    return float(h.iloc[-1]["close"])


def _fetch_usdvnd() -> float:
    from vnstock.explorer.misc.exchange_rate import vcb_exchange_rate
    fx = vcb_exchange_rate(date=TODAY)
    usd = fx[fx["currency_code"] == "USD"].iloc[0]
    return float(str(usd["sell"]).replace(",", ""))


def _fetch_gold() -> float:
    from vnstock.explorer.misc.gold_price import sjc_gold_price
    g = sjc_gold_price()
    hcm = g[g["branch"] == "Hồ Chí Minh"]
    row = hcm.iloc[0] if len(hcm) else g.iloc[0]
    return float(row["sell_price"])


def main() -> int:
    if os.path.isdir(PART):
        print(f"[fetch_macro] {PART} đã tồn tại -> bỏ qua")
        return 0
    try:
        row = {
            "date": TODAY,
            "gold_usd": _fetch_gold(),
            "usdvnd": _fetch_usdvnd(),
            "vnindex": _fetch_vnindex(),
        }
    except Exception as e:
        print(f"[fetch_macro] WARN vnstock fail: {e} -> bỏ qua, dùng carry-forward")
        return 0

    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.appName("fetch-macro")
             .master("local[1]").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    spark.createDataFrame([row]).write.mode("overwrite").parquet(PART)
    spark.stop()
    print(f"[fetch_macro] ghi {PART}: {row}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
