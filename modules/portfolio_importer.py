"""
Portfolio importer — supports:
  1. CAMS / Karvy / KFintech Consolidated Account Statement (CAS) PDFs
  2. CSV / Excel transaction exports (custom format)

For CAS PDFs, the file is usually password-protected (PAN+DOB or chosen password).
We use the `casparser` library which handles both CAMS and Karvy formats.
"""
from __future__ import annotations
import io
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

import pandas as pd

from . import database as db


def import_cas_pdf(file_bytes: bytes, password: str) -> Dict[str, Any]:
    """
    Parse a CAS PDF and import its transactions.
    Requires `casparser` (pip install casparser).
    Returns a summary dict with counts.
    """
    try:
        import casparser
    except ImportError:
        raise RuntimeError(
            "casparser is not installed. Run: pip install casparser"
        )

    # casparser accepts a file path or BytesIO
    bio = io.BytesIO(file_bytes)
    parsed = casparser.read_cas_pdf(bio, password, output="dict")

    rows: List[Dict[str, Any]] = []
    schemes_seen: List[Dict[str, Any]] = []

    for folio in parsed.get("folios", []):
        folio_no = folio.get("folio")
        for scheme in folio.get("schemes", []):
            scheme_name = scheme.get("scheme")
            isin = scheme.get("isin")
            amfi = scheme.get("amfi")  # AMFI scheme code — the canonical key

            if amfi:
                schemes_seen.append({
                    "scheme_code": str(amfi),
                    "scheme_name": scheme_name,
                    "isin_growth": isin,
                    "fund_house": _extract_amc_from_scheme_name(scheme_name),
                })

            for txn in scheme.get("transactions", []):
                rows.append({
                    "folio_no": folio_no,
                    "scheme_code": str(amfi) if amfi else None,
                    "scheme_name_raw": scheme_name,
                    "transaction_date": _parse_date_str(txn.get("date")),
                    "transaction_type": _normalize_txn_type(txn.get("type") or txn.get("description", "")),
                    "amount": float(txn.get("amount") or 0),
                    "units": float(txn.get("units")) if txn.get("units") else None,
                    "nav": float(txn.get("nav")) if txn.get("nav") else None,
                    "source_file": "cas_pdf",
                })

    # Save schemes (basic info only — full details get filled by VR scraper later)
    for s in {row["scheme_code"]: row for row in schemes_seen if row["scheme_code"]}.values():
        existing = db.get_scheme(s["scheme_code"])
        if not existing:
            db.upsert_scheme({
                "scheme_code": s["scheme_code"],
                "scheme_name": s["scheme_name"],
                "isin_growth": s.get("isin_growth"),
                "fund_house": s.get("fund_house"),
                "vr_code": None,
                "isin_div": None,
                "category": None,
                "sub_category": None,
                "objective": None,
                "benchmark": None,
                "expense_ratio": None,
                "aum": None,
            })

    inserted = db.insert_transactions(rows)
    return {
        "schemes_found": len({r["scheme_code"] for r in schemes_seen if r["scheme_code"]}),
        "transactions_in_file": len(rows),
        "transactions_inserted": inserted,
        "duplicates_skipped": len(rows) - inserted,
    }


def import_csv(file_bytes: bytes, column_map: Dict[str, str]) -> Dict[str, Any]:
    """
    Generic CSV/Excel transaction import.
    column_map maps OUR fields → user's column names. Required keys:
      transaction_date, scheme_name, amount
    Optional: folio_no, scheme_code, units, nav, transaction_type
    """
    try:
        df = pd.read_csv(io.BytesIO(file_bytes))
    except Exception:
        df = pd.read_excel(io.BytesIO(file_bytes))

    required = ["transaction_date", "scheme_name", "amount"]
    for k in required:
        if k not in column_map or column_map[k] not in df.columns:
            raise ValueError(f"Missing required column mapping: {k}")

    rows = []
    for _, r in df.iterrows():
        try:
            amount = float(r[column_map["amount"]])
        except (ValueError, TypeError):
            continue
        if amount <= 0:
            continue
        rows.append({
            "folio_no": str(r[column_map["folio_no"]]) if column_map.get("folio_no") else None,
            "scheme_code": str(r[column_map["scheme_code"]]) if column_map.get("scheme_code") else None,
            "scheme_name_raw": str(r[column_map["scheme_name"]]),
            "transaction_date": _parse_date_str(r[column_map["transaction_date"]]),
            "transaction_type": _normalize_txn_type(
                str(r[column_map["transaction_type"]]) if column_map.get("transaction_type") else "Purchase"
            ),
            "amount": amount,
            "units": float(r[column_map["units"]]) if column_map.get("units") and pd.notna(r[column_map["units"]]) else None,
            "nav": float(r[column_map["nav"]]) if column_map.get("nav") and pd.notna(r[column_map["nav"]]) else None,
            "source_file": "csv_import",
        })

    inserted = db.insert_transactions(rows)
    return {
        "transactions_in_file": len(rows),
        "transactions_inserted": inserted,
        "duplicates_skipped": len(rows) - inserted,
    }


# --------------------------- helpers ---------------------------

def _parse_date_str(d) -> str:
    """Try a few common formats, return ISO date string."""
    if isinstance(d, datetime):
        return d.date().isoformat()
    if hasattr(d, "isoformat"):
        return d.isoformat()[:10]
    s = str(d).strip()
    for fmt in ("%Y-%m-%d", "%d-%b-%Y", "%d-%m-%Y", "%d/%m/%Y", "%d-%b-%y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    # last resort — pandas
    return pd.to_datetime(s, dayfirst=True).date().isoformat()


def _normalize_txn_type(raw: str) -> str:
    """Map raw description strings to a controlled vocabulary."""
    if not raw:
        return "Purchase"
    r = raw.lower()
    if any(k in r for k in ["sip", "systematic"]):
        return "SIP"
    if any(k in r for k in ["redempt", "sell", "sale", "swp", "switch out"]):
        return "Redemption"
    if any(k in r for k in ["dividend", "idcw"]):
        return "Dividend"
    if any(k in r for k in ["switch in", "switch_in"]):
        return "Switch-In"
    if any(k in r for k in ["purchase", "buy", "subscript", "investment"]):
        return "Purchase"
    return "Purchase"


def _extract_amc_from_scheme_name(name: str) -> Optional[str]:
    """
    Cheap heuristic — first 2-3 words of scheme name typically match the AMC.
    e.g. 'HDFC Mid-Cap Opportunities Fund' → 'HDFC'
    Replace this with a proper AMC lookup table for production.
    """
    if not name:
        return None
    # well-known AMCs that are the first word
    known = ["HDFC", "ICICI", "SBI", "Axis", "Kotak", "Nippon", "DSP", "Mirae",
             "Aditya", "Franklin", "UTI", "Tata", "Quant", "PPFAS", "Parag",
             "Edelweiss", "Invesco", "L&T", "Sundaram", "Canara", "IDFC",
             "Bandhan", "Motilal", "Baroda", "PGIM", "Mahindra", "JM", "WhiteOak",
             "Bank of India", "HSBC", "Navi", "Quantum", "ITI", "Helios",
             "Old Bridge", "Samco", "360 ONE", "Trust", "Union", "Zerodha"]
    for k in known:
        if name.lower().startswith(k.lower()):
            return k
    return name.split()[0] if name else None
