import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import streamlit as st
import db

st.set_page_config(page_title="Nhập CPI", layout="centered")
st.title("📝 Nhập CPI / Lãi suất theo tháng")

try:
    macro = db.load_macro()
except Exception as e:
    st.error(f"Không kết nối được Postgres: {e}")
    st.stop()

existing = {r.month: (r.cpi, r.rate) for r in macro.itertuples()}
options = db.month_options()
# default = most recent existing month, else the latest offered month
default_idx = (options.index(macro["month"].max())
               if not macro.empty and macro["month"].max() in options
               else len(options) - 1)

month = st.selectbox("Tháng", options, index=default_idx)
prefill = existing.get(month, (0.0, 0.0))
mode = "chỉnh sửa" if month in existing else "thêm mới"
st.caption(f"Chế độ: **{mode}** — "
           + ("cập nhật giá trị đã có" if month in existing else "tháng mới"))

with st.form("cpi_form"):
    cpi = st.number_input("Chỉ số CPI", min_value=0.0,
                          value=float(prefill[0]), step=0.1)
    rate = st.number_input("Lãi suất (%/năm)", min_value=0.0,
                           value=float(prefill[1]), step=0.1)
    submitted = st.form_submit_button("Lưu")

if submitted:
    if cpi <= 0 or rate <= 0:
        st.error("CPI và lãi suất phải là số dương.")
    else:
        try:
            db.upsert_macro(month, cpi, rate)
            st.success(f"Đã lưu {month}: cpi={cpi}, rate={rate}")
            macro = db.load_macro()
        except Exception as e:
            st.error(f"Lưu thất bại: {e}")

st.subheader("Dữ liệu vĩ mô hiện tại")
st.dataframe(
    macro, use_container_width=True, hide_index=True,
    column_config={
        "month": "Tháng",
        "cpi": "Chỉ số CPI",
        "rate": "Lãi suất (%/năm)",
    },
)
if not macro.empty:
    chart = macro.set_index("month")[["cpi", "rate"]].rename(
        columns={"cpi": "Chỉ số CPI", "rate": "Lãi suất (%/năm)"})
    chart.index.name = "Tháng"
    st.line_chart(chart)
