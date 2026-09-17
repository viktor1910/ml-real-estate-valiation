"""Tải chuỗi vĩ mô nhịp ngày cho đồ án Real Estate Valuation (M6).

3 chuỗi:
  - gold_usd : giá vàng thế giới (yfinance GC=F, USD/oz)
  - usdvnd   : tỷ giá USD/VND (yfinance VND=X)
  - vnindex  : chỉ số VN-Index (vnstock, nguồn VCI)

Xuất:
  data/raw_csv/macro/gold_usd.csv, usdvnd.csv, vnindex.csv  (từng chuỗi thô)
  data/raw_csv/macro/macro_daily.csv                        (gộp, lịch ngày, ffill)

macro_daily reindex về lịch NGÀY liên tục rồi forward-fill: giá trị mỗi ngày =
giá trị phiên gần nhất đã biết → khớp as-of join theo `posted_at` (M6), không leakage.

Chạy:
  python ml/fetch_macro.py                      # 2018-01-01 -> hôm nay
  python ml/fetch_macro.py --start 2020-01-01 --end 2026-09-18
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import pandas as pd

OUT_DIR = Path(__file__).resolve().parent.parent / "data" / "raw_csv" / "macro"


def _close_series(df: pd.DataFrame, name: str) -> pd.Series:
    """Lấy cột Close từ kết quả yfinance, phẳng về Series (index=Date)."""
    if df is None or df.empty:
        raise SystemExit(f"[FAIL] yfinance trả rỗng cho {name}")
    close = df["Close"]
    if isinstance(close, pd.DataFrame):  # MultiIndex 1 ticker -> 1 cột
        close = close.iloc[:, 0]
    s = close.dropna()
    s.index = pd.to_datetime(s.index).tz_localize(None).normalize()
    s.name = name
    return s


def fetch_yf(ticker: str, name: str, start: str, end: str) -> pd.Series:
    import yfinance as yf

    df = yf.download(ticker, start=start, end=end, progress=False, auto_adjust=True)
    return _close_series(df, name)


def fetch_vnindex(start: str, end: str) -> pd.Series:
    from vnstock import Vnstock

    df = Vnstock().stock(symbol="VNINDEX", source="VCI").quote.history(
        start=start, end=end, interval="1D"
    )
    if df is None or df.empty:
        raise SystemExit("[FAIL] vnstock trả rỗng cho VNINDEX")
    s = df.set_index(pd.to_datetime(df["time"]))["close"].dropna()
    s.index = s.index.tz_localize(None).normalize()
    s.name = "vnindex"
    return s


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2018-01-01")
    ap.add_argument("--end", default=date.today().isoformat())
    args = ap.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    series = {
        "gold_usd": lambda: fetch_yf("GC=F", "gold_usd", args.start, args.end),
        "usdvnd": lambda: fetch_yf("VND=X", "usdvnd", args.start, args.end),
        "vnindex": lambda: fetch_vnindex(args.start, args.end),
    }

    frames = []
    for name, fn in series.items():
        s = fn()
        s.to_csv(OUT_DIR / f"{name}.csv")
        print(f"[OK] {name}: {len(s)} phiên, {s.index.min().date()} -> {s.index.max().date()}")
        frames.append(s)

    # gộp + reindex lịch ngày liên tục + forward-fill
    merged = pd.concat(frames, axis=1).sort_index()
    full = pd.date_range(merged.index.min(), merged.index.max(), freq="D")
    merged = merged.reindex(full).ffill()
    merged.index.name = "date"

    missing = merged.isna().sum()
    if missing.any():
        print(f"[WARN] còn NaN đầu chuỗi (trước phiên đầu tiên): {missing.to_dict()}")

    out = OUT_DIR / "macro_daily.csv"
    merged.to_csv(out)
    print(f"[OK] macro_daily: {len(merged)} ngày, {merged.index.min().date()} -> {merged.index.max().date()}")
    print(f"[OK] ghi {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
