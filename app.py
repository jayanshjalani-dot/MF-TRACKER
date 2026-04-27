"""
MF Portfolio Tracker — main entry point.

Run locally:
    streamlit run app.py

Streamlit Cloud will use this file automatically when deployed from a GitHub repo.
"""
import streamlit as st
import pandas as pd

from modules import database as db

st.set_page_config(
    page_title="MF Portfolio Tracker",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Initialize the database on first launch
db.init_db()

st.title("📊 Mutual Fund Portfolio Tracker")
st.caption("Track holdings changes, sector shifts, fund manager moves, and news for your mutual fund schemes.")

# --------------------------- Top KPIs ---------------------------
held_schemes = db.list_held_schemes()
active_sips = db.list_active_sips()
unread_alerts = db.list_alerts(unread_only=True, limit=500)

col1, col2, col3, col4 = st.columns(4)
col1.metric("Schemes Held", len(held_schemes))
col2.metric("Active SIPs", len(active_sips))
col3.metric("Unread Alerts", len(unread_alerts),
            delta="!" if unread_alerts else None,
            delta_color="inverse" if unread_alerts else "off")
with db.get_conn() as conn:
    txn_count = conn.execute("SELECT COUNT(*) AS c FROM transactions").fetchone()["c"]
col4.metric("Transactions", txn_count)

st.divider()

# --------------------------- Quick start guide for new users ---------------------------
if not held_schemes:
    st.info(
        "👋 **Welcome!** No portfolio detected yet. Get started:\n\n"
        "1. Go to **📥 Import Portfolio** in the sidebar\n"
        "2. Upload your CAS PDF (from CAMS/KFintech) or a CSV of transactions\n"
        "3. Run **🔄 Refresh Data** to fetch fund details and factsheets\n"
        "4. Explore **📈 Holdings Changes**, **🏢 Sector Analysis**, and **📰 News**"
    )
else:
    st.subheader("Your Portfolio")

    portfolio_rows = []
    with db.get_conn() as conn:
        for s in held_schemes:
            net_invested = conn.execute(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN transaction_type IN ('Purchase', 'SIP', 'Switch-In')
                                      THEN amount ELSE 0 END), 0) -
                    COALESCE(SUM(CASE WHEN transaction_type = 'Redemption'
                                      THEN amount ELSE 0 END), 0) AS net
                FROM transactions WHERE scheme_code = ?
                """, (s["scheme_code"],)
            ).fetchone()["net"]

            units = conn.execute(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN transaction_type IN ('Purchase', 'SIP', 'Switch-In')
                                      THEN units ELSE 0 END), 0) -
                    COALESCE(SUM(CASE WHEN transaction_type = 'Redemption'
                                      THEN units ELSE 0 END), 0) AS u
                FROM transactions WHERE scheme_code = ?
                """, (s["scheme_code"],)
            ).fetchone()["u"]

            portfolio_rows.append({
                "Scheme": s["scheme_name"],
                "Category": s["sub_category"] or s["category"] or "—",
                "Fund House": s["fund_house"] or "—",
                "Units": round(units, 3),
                "Invested (₹)": round(net_invested, 2),
            })

    df = pd.DataFrame(portfolio_rows)
    st.dataframe(df, use_container_width=True, hide_index=True)

    if unread_alerts:
        st.subheader(f"🔔 Recent Alerts ({len(unread_alerts)} unread)")
        for a in unread_alerts[:5]:
            severity_emoji = {"info": "ℹ️", "warning": "⚠️", "critical": "🚨"}.get(a["severity"], "•")
            st.warning(f"{severity_emoji} **{a['title']}** — {a['description']}")
        if len(unread_alerts) > 5:
            st.caption(f"+ {len(unread_alerts) - 5} more — see the Alerts page")

st.divider()
st.caption(
    "💡 Data sources: AMFI (scheme master + NAV) · mfapi.in (NAV history) · "
    "Value Research Online (factsheets, holdings) · Google News RSS (news). "
    "Factsheets are typically published 7-10 days after month-end."
)
