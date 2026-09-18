#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
promote — Piece 4. Gate promote-if-better. KHÔNG đè model cũ mù.

So baseline_rmse của model MỚI vs model PROD hiện tại (models/current.json).
- MỚI tốt hơn (rmse <= cũ)  -> cập nhật con trỏ current.json = version mới.
- MỚI tệ hơn               -> GIỮ cũ, log cảnh báo (rollback tự nhiên).
- Chưa có con trỏ (lần đầu) -> promote vô điều kiện.

Không dùng Spark — chỉ đọc metrics.json. Deterministic (Rule 5: code, không cần model).

Run:
    python3 promote.py --new-version 2026-09-18
    python3 promote.py --new-version 2026-09-18 --pointer models/current.json
"""

import argparse
import json
import os


def _rmse(version):
    p = f"models/v{version}/metrics.json"
    with open(p, encoding="utf-8") as f:
        return json.load(f)["baseline_rmse"]


def main():
    ap = argparse.ArgumentParser(description="Promote model mới nếu tốt hơn prod")
    ap.add_argument("--new-version", required=True, help="Version model vừa train, vd 2026-09-18")
    ap.add_argument("--pointer", default="models/current.json")
    args = ap.parse_args()

    new_rmse = _rmse(args.new_version)

    if not os.path.exists(args.pointer):
        _write(args.pointer, args.new_version, new_rmse)
        print(f"🟢 PROMOTE (lần đầu): v{args.new_version} rmse={new_rmse:.3f} -> {args.pointer}")
        return

    cur = json.load(open(args.pointer, encoding="utf-8"))["version"]
    if cur == args.new_version:
        print(f"= con trỏ đã trỏ v{cur}, bỏ qua")
        return
    cur_rmse = _rmse(cur)

    if new_rmse <= cur_rmse:
        _write(args.pointer, args.new_version, new_rmse)
        print(f"🟢 PROMOTE: v{args.new_version} rmse={new_rmse:.3f} <= v{cur} rmse={cur_rmse:.3f}")
    else:
        print(f"🔴 GIỮ CŨ: v{args.new_version} rmse={new_rmse:.3f} > v{cur} rmse={cur_rmse:.3f} "
              f"→ model mới TỆ hơn, không promote")


def _write(pointer, version, rmse):
    import datetime
    with open(pointer, "w", encoding="utf-8") as f:
        json.dump({"version": version, "baseline_rmse": round(rmse, 3),
                   "promoted_at": datetime.datetime.now().isoformat(timespec="seconds")},
                  f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
