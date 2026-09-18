#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""fetch_macro — lấy market series (vnstock) append macro_raw lake.

Series: vnindex (VCI close), usdvnd (VCB sell), gold_usd (SJC sell HCM —
tên cột giữ theo schema cũ dù đơn vị VND/lượng, model dùng như 1 tín hiệu
vĩ mô).

Chế độ:
  (mặc định)            fetch hôm nay -> macro_raw/dt=<today>
  --backfill START END  fetch dải quá khứ qua API -> macro_raw/dt=backfill-<END>
                        (vnindex 1 call range; usdvnd/gold loop từng ngày)

CPI/lãi suất KHÔNG ở đây — chúng monthly, lưu ở bảng Postgres `macro_monthly`
(nhập/UPSERT qua SQL hoặc UI; xem sql/init.sql). macro_features đọc bảng đó.
vnstock fail (chế độ daily) -> exit 0 + WARN (daily loop không vỡ; carry-forward).
"""
import os
import sys
import time
import argparse
import datetime

if os.path.basename(os.getcwd()) == "ml":
    os.chdir("..")
from config import build_spark, LAKE as _LAKE_ROOT

LAKE = f"{_LAKE_ROOT}/macro_raw"
TODAY = datetime.date.today().isoformat()


def _vnindex_history(start: str, end: str) -> dict:
    """{date_iso: close} cho dải [start,end] — 1 call."""
    from vnstock import Quote
    h = Quote(symbol="VNINDEX", source="VCI").history(start=start, end=end, interval="1D")
    return {str(r["time"])[:10]: float(r["close"]) for _, r in h.iterrows()}


def _usdvnd_on(day: str) -> float | None:
    from vnstock.explorer.misc.exchange_rate import vcb_exchange_rate
    try:
        fx = vcb_exchange_rate(date=day)
        usd = fx[fx["currency_code"] == "USD"]
        return float(str(usd.iloc[0]["sell"]).replace(",", "")) if len(usd) else None
    except Exception:
        return None


def _gold_on(day: str) -> float | None:
    from vnstock.explorer.misc.gold_price import sjc_gold_price
    try:
        g = sjc_gold_price(date=day)
        hcm = g[g["branch"] == "Hồ Chí Minh"]
        row = hcm.iloc[0] if len(hcm) else (g.iloc[0] if len(g) else None)
        return float(row["sell_price"]) if row is not None else None
    except Exception:
        return None


def _write(rows: list[dict], part: str) -> None:
    spark = build_spark("fetch-macro")
    spark.createDataFrame(rows).write.mode("overwrite").parquet(part)
    spark.stop()


def daily() -> int:
    part = f"{LAKE}/dt={TODAY}"
    if os.path.isdir(part):
        print(f"[fetch_macro] {part} đã tồn tại -> bỏ qua")
        return 0
    try:
        vni = _vnindex_history((datetime.date.today() - datetime.timedelta(days=10)).isoformat(), TODAY)
        row = {
            "date": TODAY,
            "gold_usd": _gold_on(TODAY),
            "usdvnd": _usdvnd_on(TODAY),
            "vnindex": vni.get(TODAY) or list(vni.values())[-1],
        }
    except Exception as e:
        print(f"[fetch_macro] WARN vnstock fail: {e} -> bỏ qua, dùng carry-forward")
        return 0
    _write([row], part)
    print(f"[fetch_macro] ghi {part}: {row}")
    return 0


def backfill(start: str, end: str, sleep: float = 2.2) -> int:
    """Fetch dải quá khứ. vnindex forward-fill sang ngày không có phiên
    (weekend/lễ) bằng giá trị phiên gần nhất trước đó.

    Throttle: mỗi ngày = 2 call (fx+gold). sleep ~2.2s/ngày giữ dưới giới
    hạn Community 60 req/phút (~54/phút). Có API key sponsor -> giảm sleep.
    """
    vni_hist = _vnindex_history(start, end)
    d0 = datetime.date.fromisoformat(start)
    d1 = datetime.date.fromisoformat(end)
    rows, last_vni = [], None
    n = (d1 - d0).days + 1
    for i in range(n):
        day = (d0 + datetime.timedelta(days=i)).isoformat()
        if day in vni_hist:
            last_vni = vni_hist[day]
        rows.append({
            "date": day,
            "gold_usd": _gold_on(day),
            "usdvnd": _usdvnd_on(day),
            "vnindex": last_vni,
        })
        time.sleep(sleep)   # throttle rate limit
        if (i + 1) % 30 == 0:
            print(f"[backfill] {i + 1}/{n} ngày...")
    part = f"{LAKE}/dt=backfill-{end}"
    _write(rows, part)
    got_v = sum(1 for r in rows if r["vnindex"] is not None)
    got_u = sum(1 for r in rows if r["usdvnd"] is not None)
    got_g = sum(1 for r in rows if r["gold_usd"] is not None)
    print(f"[backfill] ghi {part}: {len(rows)} ngày "
          f"(vnindex={got_v}, usdvnd={got_u}, gold={got_g} có giá trị)")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--backfill", nargs=2, metavar=("START", "END"))
    ap.add_argument("--sleep", type=float, default=2.2,
                    help="giây nghỉ giữa mỗi ngày (rate limit); giảm nếu có key sponsor")
    a = ap.parse_args()
    if a.backfill:
        return backfill(a.backfill[0], a.backfill[1], a.sleep)
    return daily()


if __name__ == "__main__":
    sys.exit(main())
