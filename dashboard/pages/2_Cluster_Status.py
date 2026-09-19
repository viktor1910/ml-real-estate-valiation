import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import pandas as pd
import streamlit as st

import auth
import cluster

st.set_page_config(page_title="Trạng thái cụm", layout="wide")
auth.require_login()
st.title("🖥️ Trạng thái cụm Spark")

ui_url = os.environ.get("SPARK_MASTER_UI", cluster.DEFAULT_UI)

left, mid, right = st.columns([3, 1, 1])
left.caption(f"Nguồn: `{ui_url}/json/` — đặt biến môi trường `SPARK_MASTER_UI` nếu master ở IP khác.")
auto = mid.toggle("Tự làm mới 5s", value=False, help="Bật khi demo: tắt 1 worker sẽ tự hiện DEAD, không cần bấm.")
if right.button("🔄 Làm mới"):
    st.rerun()

try:
    data = cluster.fetch_master_json(ui_url)
except Exception as e:
    st.error(f"Không kết nối được Spark master ({ui_url}): {e}")
    st.stop()

p = cluster.parse_master(data)

# --- Distributed proof banner --------------------------------------------
# Điểm chốt của demo: đếm số máy vật lý riêng (host IP khác nhau) đang sống.
n_hosts = cluster.distinct_hosts(p["workers"])
if n_hosts >= 2:
    st.success(
        f"✅ Đang chạy PHÂN TÁN trên **{n_hosts} máy vật lý** riêng biệt "
        f"({', '.join(sorted({w['host'] for w in p['workers'] if w['state'] == 'ALIVE'}))}). "
        "Mỗi worker là một IP/máy khác nhau — không phải nhiều tiến trình trên một máy."
    )
elif n_hosts == 1:
    st.warning("⚠️ Chỉ 1 máy vật lý đang sống — chưa thể hiện phân tán. Kiểm tra worker còn lại.")
else:
    st.error("🔴 Không có worker sống nào — cụm chưa sẵn sàng.")

# --- Master summary -------------------------------------------------------
badge = "🟢" if p["status"] == "ALIVE" else "🔴"
st.subheader(f"{badge} Master: {p['status']}")
c1, c2, c3 = st.columns(3)
c1.metric("Worker sống / tổng", f"{p['workers_alive']} / {p['workers_total']}")
c2.metric("Cores đang dùng", f"{p['cores_used']} / {p['cores']}")
c3.metric("RAM đang dùng",
          f"{cluster.fmt_mb(p['memory_used'])} / {cluster.fmt_mb(p['memory'])}")

# --- Workers --------------------------------------------------------------
st.subheader("Danh sách worker")
if p["workers"]:
    rows = [
        {
            "Worker (máy)": f"{w['host']}:{w['port']}",
            "Trạng thái": ("🟢 " if w["state"] == "ALIVE" else "🔴 ") + w["state"],
            "Đang chạy task": "⚡ Có" if w["cores_used"] > 0 else "—",
            "Cores": f"{w['cores_used']} / {w['cores']}",
            "RAM": f"{cluster.fmt_mb(w['memory_used'])} / {cluster.fmt_mb(w['memory'])}",
            "Heartbeat": f"{cluster.heartbeat_age(w['lastheartbeat'])}s trước",
        }
        for w in p["workers"]
    ]
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
else:
    st.warning("Không có worker nào đăng ký với master.")

# --- Running applications -------------------------------------------------
st.subheader("Ứng dụng đang chạy")
st.caption(
    "Khi job chạy, cores của app được lấy từ nhiều worker cùng lúc — "
    "cột \"Đang chạy task\" ở bảng trên sẽ ⚡ trên từng máy tham gia."
)
if p["apps"]:
    rows = [
        {
            "ID": a["id"],
            "Tên": a["name"],
            "User": a["user"],
            "Cores": a["cores"],
            "RAM/executor": cluster.fmt_mb(a["memory_per_executor"]),
            "Trạng thái": a["state"],
        }
        for a in p["apps"]
    ]
    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
else:
    st.info("Hiện không có ứng dụng nào đang chạy trên cụm.")

# --- Auto-refresh (demo) --------------------------------------------------
# Không thêm dependency: chỉ ngủ ngắn rồi rerun. Đủ cho demo tắt/bật worker.
if auto:
    time.sleep(5)
    st.rerun()
