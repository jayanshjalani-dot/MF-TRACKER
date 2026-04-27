import streamlit as st
import pandas as pd
import io

from modules import database as db
from modules import portfolio_importer
from modules import sip_detector

st.set_page_config(page_title="Import Portfolio", page_icon="📥", layout="wide")
st.title("📥 Import Portfolio")

tab_pdf, tab_csv, tab_diagnose = st.tabs(["CAS PDF", "CSV / Excel", "🔧 SIP Diagnostics"])

# ---------------------------------------------------------------- CAS PDF
with tab_pdf:
    st.markdown(
        """
        Upload your **Consolidated Account Statement (CAS)** PDF from CAMS or KFintech.

        **How to get one:**
        - CAMS: https://www.camsonline.com → Investor Services → Mailback Services → CAS
        - KFintech: https://mfs.kfintech.com → Investor Services → CAS

        Pick the **detailed** statement (not summary). Choose a password and remember it.
        """
    )

    pdf_file = st.file_uploader("Upload CAS PDF", type=["pdf"])
    password = st.text_input("PDF Password", type="password",
                             help="Whatever password you set when requesting the CAS")

    if st.button("Import PDF", type="primary", disabled=not (pdf_file and password)):
        with st.spinner("Parsing CAS PDF..."):
            try:
                result = portfolio_importer.import_cas_pdf(pdf_file.read(), password)
                st.success(
                    f"✅ Imported {result['transactions_inserted']} new transactions "
                    f"across {result['schemes_found']} schemes "
                    f"({result['duplicates_skipped']} duplicates skipped)"
                )

                with st.spinner("Detecting SIPs..."):
                    sip_result = sip_detector.detect_sips()
                st.info(
                    f"🔍 SIP detection: found **{sip_result['sips_found']} SIPs**, "
                    f"marked **{sip_result['transactions_marked']}** transactions as SIP instalments"
                )
            except RuntimeError as e:
                st.error(str(e))
                st.code("pip install casparser")
            except Exception as e:
                st.error(f"Import failed: {e}")

# ---------------------------------------------------------------- CSV/Excel
with tab_csv:
    st.markdown("Upload a CSV or Excel with your transactions. You'll map columns next.")
    csv_file = st.file_uploader("Upload CSV / Excel", type=["csv", "xls", "xlsx"], key="csv_up")

    if csv_file:
        raw = csv_file.read()
        try:
            df = pd.read_csv(io.BytesIO(raw))
        except Exception:
            df = pd.read_excel(io.BytesIO(raw))

        st.write("**Preview:**", df.head(5))
        cols = ["—"] + list(df.columns)

        with st.form("col_map"):
            c1, c2, c3 = st.columns(3)
            map_date = c1.selectbox("Transaction date column *", cols, index=0)
            map_scheme = c2.selectbox("Scheme name column *", cols, index=0)
            map_amount = c3.selectbox("Amount column *", cols, index=0)
            c4, c5, c6 = st.columns(3)
            map_folio = c4.selectbox("Folio number column", cols, index=0)
            map_units = c5.selectbox("Units column", cols, index=0)
            map_nav = c6.selectbox("NAV column", cols, index=0)
            c7, c8 = st.columns(2)
            map_type = c7.selectbox("Transaction type column", cols, index=0)
            map_code = c8.selectbox("AMFI scheme code column", cols, index=0)

            submitted = st.form_submit_button("Import CSV", type="primary")

        if submitted:
            if "—" in (map_date, map_scheme, map_amount):
                st.error("Date, scheme name, and amount are required")
            else:
                column_map = {
                    "transaction_date": map_date,
                    "scheme_name": map_scheme,
                    "amount": map_amount,
                }
                if map_folio != "—": column_map["folio_no"] = map_folio
                if map_units != "—": column_map["units"] = map_units
                if map_nav != "—": column_map["nav"] = map_nav
                if map_type != "—": column_map["transaction_type"] = map_type
                if map_code != "—": column_map["scheme_code"] = map_code

                try:
                    result = portfolio_importer.import_csv(raw, column_map)
                    st.success(
                        f"✅ Imported {result['transactions_inserted']} transactions "
                        f"({result['duplicates_skipped']} duplicates skipped)"
                    )
                    sip_result = sip_detector.detect_sips()
                    st.info(f"🔍 Found {sip_result['sips_found']} SIPs")
                except Exception as e:
                    st.error(f"Import failed: {e}")

# ---------------------------------------------------------------- SIP diagnostics
with tab_diagnose:
    st.markdown(
        "If you believe a SIP **wasn't detected** that should have been, use this tool to inspect "
        "the clustering. It shows you exactly why the algorithm did or didn't flag a series of "
        "transactions as a SIP."
    )

    with db.get_conn() as conn:
        groups = conn.execute(
            """
            SELECT folio_no, scheme_name_raw, COUNT(*) AS n,
                   MIN(transaction_date) AS first_date, MAX(transaction_date) AS last_date
            FROM transactions
            WHERE amount > 0
            GROUP BY folio_no, scheme_name_raw
            HAVING n >= 2
            ORDER BY n DESC
            """
        ).fetchall()

    if not groups:
        st.info("Import some transactions first")
    else:
        choice = st.selectbox(
            "Pick a folio + scheme to diagnose",
            options=range(len(groups)),
            format_func=lambda i: f"{groups[i]['folio_no']} | {groups[i]['scheme_name_raw']} ({groups[i]['n']} txns)"
        )

        if st.button("Diagnose"):
            g = groups[choice]
            result = sip_detector.explain_grouping(g["folio_no"], g["scheme_name_raw"])
            st.json(result)
