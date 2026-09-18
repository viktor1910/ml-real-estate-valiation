import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
import pandas as pd
import streamlit as st
import auth
import db

st.set_page_config(page_title="Báo cáo Bất động sản", layout="wide")
auth.require_login()
st.title("📊 Báo cáo dự đoán")

try:
    batches = db.list_batches()
except Exception as e:
    st.error(f"Không kết nối được Postgres: {e}")
    st.stop()

if batches.empty:
    st.info("Chưa có dự đoán nào.")
    st.stop()

labels = [f"{r.model_version} · {r.scored_at}" for r in batches.itertuples()]
idx = st.selectbox(
    "Đợt chấm điểm", range(len(labels)),
    format_func=lambda i: labels[i],  # default 0 = latest (scored_at DESC)
)
sel = batches.iloc[idx]
df = db.load_predictions(sel.model_version, sel.scored_at)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Số dòng đã chấm", len(df))
c2.metric("% định giá thấp",
          f"{100 * df['is_undervalued'].mean():.1f}%" if len(df) else "0%")
c3.metric("Giá dự đoán TB (triệu đ/m²)",
          f"{df['predicted_ppm2'].mean():.1f}" if len(df) else "—")
c4.metric("Mô hình", str(sel.model_version))

districts = sorted(df["district"].dropna().unique())
pick = st.multiselect("Quận/Huyện", districts, default=districts)
only_uv = st.checkbox("Chỉ định giá thấp", value=False)
view = df[df["district"].isin(pick)]
if only_uv:
    view = view[view["is_undervalued"]]

# % thay cho số thập phân trong bảng (chỉ đổi cách hiển thị)
show = view.copy()
show["undervalued_ratio"] = (show["undervalued_ratio"] * 100).round(1)
st.dataframe(
    show,
    column_config={
        "district": "Quận/Huyện",
        "area_m2": "Diện tích (m²)",
        "listing_ppm2": "Giá thực (triệu đ/m²)",
        "predicted_ppm2": "Giá dự đoán (triệu đ/m²)",
        "undervalued_ratio": "Tỷ lệ định giá thấp (%)",
        "is_undervalued": "Định giá thấp",
        "url": st.column_config.LinkColumn("Liên kết"),
        "model_version": "Phiên bản mô hình",
        "scored_at": "Thời điểm chấm",
    },
    use_container_width=True, hide_index=True,
)

st.subheader("Biểu đồ")
if view.empty:
    st.caption("Không có dòng nào khớp bộ lọc.")
else:
    # 1) Giá thực vs dự đoán trung bình theo quận/huyện
    st.markdown("**Giá trung bình: thực tế so với dự đoán (triệu đ/m²)**")
    st.caption("Cột 'Giá thực' thấp hơn 'Giá dự đoán' → khu vực đang bị định giá thấp.")
    by_d = (view.groupby("district")[["listing_ppm2", "predicted_ppm2"]]
                .mean().round(2)
                .rename(columns={"listing_ppm2": "Giá thực",
                                 "predicted_ppm2": "Giá dự đoán"}))
    by_d.index.name = "Quận/Huyện"
    st.bar_chart(by_d, stack=False, x_label="Quận/Huyện", y_label="triệu đ/m²")

    # 2) Số BĐS định giá thấp theo quận/huyện
    st.markdown("**Số bất động sản định giá thấp theo quận/huyện**")
    st.caption("Cờ định giá thấp = giá thực rẻ hơn dự đoán ≥ 20%.")
    uv = (view[view["is_undervalued"]].groupby("district").size()
              .sort_values(ascending=True))
    uv.index.name = "Quận/Huyện"
    if uv.empty:
        st.caption("Không có BĐS nào bị định giá thấp trong bộ lọc hiện tại.")
    else:
        st.bar_chart(uv.rename("Số BĐS"), horizontal=True,
                     x_label="Số BĐS", y_label="Quận/Huyện")

    # 3) Phân bố mức chênh lệch giá (%)
    st.markdown("**Phân bố mức chênh lệch giá so với dự đoán (%)**")
    st.caption("(Dự đoán − thực)/dự đoán. Dương = rẻ hơn dự đoán; âm = đắt hơn.")
    pct = view["undervalued_ratio"] * 100
    bins = [-1e9, -20, -10, 0, 10, 20, 1e9]
    labels = ["< −20%", "−20…−10%", "−10…0%", "0…10%", "10…20%", "> 20%"]
    dist = (pd.cut(pct, bins=bins, labels=labels)
              .value_counts().reindex(labels))
    dist.index.name = "Mức chênh lệch"
    st.bar_chart(dist.rename("Số BĐS"),
                 x_label="Mức chênh lệch", y_label="Số BĐS")
