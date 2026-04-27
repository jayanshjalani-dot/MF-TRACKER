import streamlit as st

from modules import database as db
from modules import vr_scraper
from modules import news_aggregator

st.set_page_config(page_title="Refresh Data", page_icon="🔄", layout="wide")
st.title("🔄 Refresh Data")

st.markdown(
    """
    Pulls the latest factsheet, fund details, and managers for each held scheme.
    Run this **once a month** after the factsheet date (typically 7-10 days after month-end),
    or rely on the GitHub Actions workflow which does it automatically on the 8th.
    """
)

held = db.list_held_schemes()
if not held:
    st.warning("No portfolio imported yet.")
    st.stop()

st.caption(f"You hold **{len(held)} schemes**. Estimated time: **{len(held) * 6} seconds** (rate-limited at 3s/request).")

c1, c2 = st.columns(2)

with c1:
    if st.button("🔄 Refresh All Schemes", type="primary", use_container_width=True):
        progress = st.progress(0)
        status = st.empty()
        results_box = st.container()

        for i, s in enumerate(held):
            status.write(f"Refreshing **{s['scheme_name']}**...")
            try:
                result = vr_scraper.refresh_scheme(s["scheme_code"])
                with results_box:
                    if "error" in result:
                        st.error(f"❌ {s['scheme_name']}: {result['error']}")
                    else:
                        st.success(
                            f"✅ {s['scheme_name']} — "
                            f"{result['holdings_count']} holdings, "
                            f"{result['sectors_count']} sectors, "
                            f"factsheet date: {result['factsheet_date']}"
                        )
            except Exception as e:
                with results_box:
                    st.error(f"❌ {s['scheme_name']}: {e}")
            progress.progress((i + 1) / len(held))

        status.write("✅ Done!")
        st.balloons()

with c2:
    if st.button("📰 Fetch News for All Held Schemes", use_container_width=True):
        with st.spinner("Pulling Google News..."):
            result = news_aggregator.fetch_news_for_all_held()
        st.success(f"Fetched {result['items_inserted']} new articles")

st.divider()
st.subheader("Refresh a single scheme")

chosen = st.selectbox(
    "Pick a scheme",
    options=[s["scheme_code"] for s in held],
    format_func=lambda c: next(s["scheme_name"] for s in held if s["scheme_code"] == c),
)
if st.button("Refresh just this one"):
    with st.spinner("Working..."):
        result = vr_scraper.refresh_scheme(chosen)
    st.json(result)
