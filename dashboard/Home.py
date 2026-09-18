import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
import streamlit as st
import db

st.set_page_config(page_title="Real Estate Report", layout="wide")
st.title("📊 Prediction Report")

try:
    batches = db.list_batches()
except Exception as e:
    st.error(f"Cannot reach Postgres: {e}")
    st.stop()

if batches.empty:
    st.info("No predictions scored yet.")
    st.stop()

labels = [f"{r.model_version} · {r.scored_at}" for r in batches.itertuples()]
idx = st.selectbox(
    "Scoring batch", range(len(labels)),
    format_func=lambda i: labels[i],  # default 0 = latest (scored_at DESC)
)
sel = batches.iloc[idx]
df = db.load_predictions(sel.model_version, sel.scored_at)

c1, c2, c3, c4 = st.columns(4)
c1.metric("Rows scored", len(df))
c2.metric("% undervalued",
          f"{100 * df['is_undervalued'].mean():.1f}%" if len(df) else "0%")
c3.metric("Avg predicted ppm2",
          f"{df['predicted_ppm2'].mean():.1f}" if len(df) else "—")
c4.metric("Model", str(sel.model_version))

districts = sorted(df["district"].dropna().unique())
pick = st.multiselect("District", districts, default=districts)
only_uv = st.checkbox("Undervalued only", value=False)
view = df[df["district"].isin(pick)]
if only_uv:
    view = view[view["is_undervalued"]]

st.dataframe(
    view,
    column_config={"url": st.column_config.LinkColumn("url")},
    use_container_width=True, hide_index=True,
)

st.subheader("Charts")
if view.empty:
    st.caption("No rows match the current filters.")
else:
    st.caption("Predicted vs listing ppm2")
    st.scatter_chart(view, x="listing_ppm2", y="predicted_ppm2")

    st.caption("Undervalued count by district")
    st.bar_chart(view[view["is_undervalued"]].groupby("district").size())

    st.caption("Predicted ppm2 distribution")
    hist = view["predicted_ppm2"].value_counts(bins=20).sort_index()
    hist.index = hist.index.astype(str)  # Interval -> str for st.bar_chart
    st.bar_chart(hist)
