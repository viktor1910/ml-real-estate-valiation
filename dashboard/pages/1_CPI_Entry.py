import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import streamlit as st
import db

st.set_page_config(page_title="CPI Entry", layout="centered")
st.title("📝 Monthly CPI / Rate Entry")

try:
    macro = db.load_macro()
except Exception as e:
    st.error(f"Cannot reach Postgres: {e}")
    st.stop()

existing = {r.month: (r.cpi, r.rate) for r in macro.itertuples()}
options = db.month_options()
# default = most recent existing month, else the latest offered month
default_idx = (options.index(macro["month"].max())
               if not macro.empty and macro["month"].max() in options
               else len(options) - 1)

month = st.selectbox("Month", options, index=default_idx)
prefill = existing.get(month, (0.0, 0.0))
mode = "edit" if month in existing else "add"
st.caption(f"Mode: **{mode}** — "
           + ("editing existing values" if mode == "edit" else "new month"))

with st.form("cpi_form"):
    cpi = st.number_input("CPI index", min_value=0.0,
                          value=float(prefill[0]), step=0.1)
    rate = st.number_input("Interest rate (%/yr)", min_value=0.0,
                           value=float(prefill[1]), step=0.1)
    submitted = st.form_submit_button("Save")

if submitted:
    if cpi <= 0 or rate <= 0:
        st.error("CPI and rate must be positive.")
    else:
        try:
            db.upsert_macro(month, cpi, rate)
            st.success(f"Saved {month}: cpi={cpi}, rate={rate}")
            macro = db.load_macro()
        except Exception as e:
            st.error(f"Save failed: {e}")

st.subheader("Current macro_monthly")
st.dataframe(macro, use_container_width=True, hide_index=True)
if not macro.empty:
    st.line_chart(macro.set_index("month")[["cpi", "rate"]])
