"""
Data fetcher for fund details, factsheets, and NAVs.

IMPORTANT: Scraping Value Research aggressively can violate their Terms of Service
and get your IP blocked. This module:
  1. Uses AMFI for canonical scheme metadata + NAVs (official, free, no ToS issues)
  2. Uses mfapi.in for historical NAV data (free, community-run, well-known)
  3. Uses Value Research as a SECONDARY source for portfolio holdings + sector data,
     with rate limiting (1 req / 3 sec) and aggressive caching

For production deployment, consider:
  - Subscribing to Value Research's data API (paid, official)
  - Or licensing data from MorningStar / CRISIL
  - Or relying purely on AMFI + manual factsheet uploads from the AMC websites
"""
from __future__ import annotations
import time
import re
import json
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Any
import logging

import requests
from bs4 import BeautifulSoup

from . import database as db

log = logging.getLogger(__name__)

CACHE_DIR = Path(__file__).resolve().parent.parent / "data" / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

VR_RATE_LIMIT_SEC = 3.0  # be polite — one request every 3 seconds
VR_BASE = "https://www.valueresearchonline.com"

USER_AGENT = (
    "Mozilla/5.0 (compatible; MFPortfolioTracker/1.0; "
    "personal-portfolio-monitoring; contact: user@example.com)"
)

_last_vr_request = 0.0


# =====================================================================
# AMFI — official scheme master + NAV
# =====================================================================

def fetch_amfi_scheme_master() -> List[Dict[str, Any]]:
    """
    AMFI publishes the canonical list of schemes daily as a pipe-delimited file.
    URL: https://www.amfiindia.com/spages/NAVAll.txt

    Format:
      Scheme Code|ISIN Div Payout/ ISIN Growth|ISIN Div Reinvestment|Scheme Name|Net Asset Value|Date
    """
    url = "https://www.amfiindia.com/spages/NAVAll.txt"
    cache_file = CACHE_DIR / f"amfi_master_{date.today().isoformat()}.txt"

    if cache_file.exists():
        text = cache_file.read_text(encoding="utf-8")
    else:
        r = requests.get(url, timeout=30, headers={"User-Agent": USER_AGENT})
        r.raise_for_status()
        text = r.text
        cache_file.write_text(text, encoding="utf-8")

    schemes = []
    current_amc = None
    current_category = None

    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("Scheme Code"):
            continue

        # AMC headers and category headers don't contain pipes
        if "|" not in line:
            # simple heuristic: AMC headers end with "Mutual Fund"
            if line.endswith("Mutual Fund"):
                current_amc = line
            else:
                current_category = line
            continue

        parts = line.split("|")
        if len(parts) < 6:
            continue
        try:
            schemes.append({
                "scheme_code": parts[0].strip(),
                "isin_growth": parts[1].strip() or None,
                "isin_div": parts[2].strip() or None,
                "scheme_name": parts[3].strip(),
                "nav": float(parts[4].strip()) if parts[4].strip() not in ("N.A.", "") else None,
                "nav_date": parts[5].strip(),
                "fund_house": current_amc,
                "category_raw": current_category,
            })
        except (ValueError, IndexError):
            continue
    return schemes


def get_nav_history(scheme_code: str) -> List[Dict[str, Any]]:
    """
    Use mfapi.in (community-run, no auth needed) for full NAV history.
    Returns list of {date, nav}.
    """
    url = f"https://api.mfapi.in/mf/{scheme_code}"
    cache_file = CACHE_DIR / f"nav_{scheme_code}_{date.today().isoformat()}.json"

    if cache_file.exists():
        return json.loads(cache_file.read_text())

    try:
        r = requests.get(url, timeout=30, headers={"User-Agent": USER_AGENT})
        r.raise_for_status()
        data = r.json()
    except (requests.RequestException, ValueError) as e:
        log.warning(f"NAV fetch failed for {scheme_code}: {e}")
        return []

    history = []
    for item in data.get("data", []):
        try:
            history.append({
                "date": datetime.strptime(item["date"], "%d-%m-%Y").date().isoformat(),
                "nav": float(item["nav"]),
            })
        except (ValueError, KeyError):
            continue

    cache_file.write_text(json.dumps(history))
    return history


# =====================================================================
# Value Research — fund details + factsheet
# =====================================================================

def _vr_throttle():
    """Enforce ≥3s between Value Research requests."""
    global _last_vr_request
    elapsed = time.time() - _last_vr_request
    if elapsed < VR_RATE_LIMIT_SEC:
        time.sleep(VR_RATE_LIMIT_SEC - elapsed)
    _last_vr_request = time.time()


def find_vr_code(scheme_name: str) -> Optional[str]:
    """
    Search Value Research for a scheme and return its VR code.
    Cached to avoid repeated lookups.
    """
    cache_file = CACHE_DIR / f"vr_search_{re.sub(r'[^a-z0-9]+', '_', scheme_name.lower())}.json"
    if cache_file.exists():
        return json.loads(cache_file.read_text()).get("vr_code")

    _vr_throttle()
    url = f"{VR_BASE}/funds/search/?q={requests.utils.quote(scheme_name)}"
    try:
        r = requests.get(url, timeout=30, headers={"User-Agent": USER_AGENT})
        r.raise_for_status()
    except requests.RequestException as e:
        log.warning(f"VR search failed for '{scheme_name}': {e}")
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    # The first /funds/ link in results is usually the best match.
    link = soup.select_one("a[href*='/funds/']")
    vr_code = None
    if link:
        m = re.search(r"/funds/(\d+)/", link["href"])
        if m:
            vr_code = m.group(1)

    cache_file.write_text(json.dumps({"vr_code": vr_code, "queried": scheme_name}))
    return vr_code


def fetch_vr_fund_page(vr_code: str) -> Optional[BeautifulSoup]:
    """Fetch a fund's main VR page. Cached for 24 hours."""
    cache_file = CACHE_DIR / f"vr_fund_{vr_code}_{date.today().isoformat()}.html"
    if cache_file.exists():
        return BeautifulSoup(cache_file.read_text(encoding="utf-8"), "html.parser")

    _vr_throttle()
    url = f"{VR_BASE}/funds/{vr_code}/"
    try:
        r = requests.get(url, timeout=30, headers={"User-Agent": USER_AGENT})
        r.raise_for_status()
    except requests.RequestException as e:
        log.warning(f"VR fund page fetch failed for {vr_code}: {e}")
        return None

    cache_file.write_text(r.text, encoding="utf-8")
    return BeautifulSoup(r.text, "html.parser")


def parse_fund_details(vr_code: str) -> Optional[Dict[str, Any]]:
    """
    Pull category, sub-category, fund manager, objective, benchmark, expense ratio, AUM.
    NOTE: VR's HTML structure changes occasionally. The selectors below may need updating.
          That's why selectors are isolated here — easy to fix in one place.
    """
    soup = fetch_vr_fund_page(vr_code)
    if not soup:
        return None

    def text_after_label(label: str) -> Optional[str]:
        """Look for a label-value pair anywhere on the page."""
        el = soup.find(string=re.compile(rf"\s*{re.escape(label)}\s*:?$", re.I))
        if el and el.parent:
            sib = el.parent.find_next_sibling()
            if sib:
                return sib.get_text(strip=True)
        return None

    def safe_select_text(selector: str) -> Optional[str]:
        el = soup.select_one(selector)
        return el.get_text(strip=True) if el else None

    return {
        "vr_code": vr_code,
        "scheme_name": safe_select_text("h1") or safe_select_text(".fund-name"),
        "category": text_after_label("Category"),
        "sub_category": text_after_label("Sub-category") or text_after_label("Sub Category"),
        "fund_house": text_after_label("Fund house") or text_after_label("AMC"),
        "benchmark": text_after_label("Benchmark"),
        "fund_managers": _parse_managers(soup),
        "objective": _parse_objective(soup),
        "expense_ratio": _parse_float(text_after_label("Expense ratio") or text_after_label("Expense Ratio")),
        "aum": _parse_aum(text_after_label("Fund size") or text_after_label("AUM")),
    }


def _parse_managers(soup: BeautifulSoup) -> List[str]:
    text = soup.get_text(" ", strip=True)
    m = re.search(r"Fund Manager[s]?\s*:?\s*([A-Z][^.]{2,150}?)(?:\s+Inception|\s+Benchmark|\.)", text)
    if m:
        names = re.split(r"\s*(?:,|\band\b|/)\s*", m.group(1))
        return [n.strip() for n in names if n.strip() and len(n.strip()) > 3]
    return []


def _parse_objective(soup: BeautifulSoup) -> Optional[str]:
    for h in soup.find_all(["h2", "h3", "h4"]):
        if "objective" in h.get_text(strip=True).lower():
            sib = h.find_next_sibling()
            if sib:
                return sib.get_text(strip=True)
    return None


def _parse_float(s: Optional[str]) -> Optional[float]:
    if not s:
        return None
    m = re.search(r"-?\d+\.?\d*", s.replace(",", ""))
    return float(m.group()) if m else None


def _parse_aum(s: Optional[str]) -> Optional[float]:
    """Convert 'Rs 12,345 Cr' → 1234500 (in lakhs)."""
    if not s:
        return None
    val = _parse_float(s)
    if val is None:
        return None
    if "cr" in s.lower():
        return val * 100  # Cr → Lakhs
    return val


def parse_portfolio(vr_code: str) -> Dict[str, Any]:
    """
    Pull the holdings + sector allocation from VR's portfolio page.
    Returns {factsheet_date, holdings: [...], sectors: [...]}.

    NOTE: Portfolio page selectors are particularly fragile — VR changes the table
    structure now and then. Update _portfolio_table_selector() if scraping fails.
    """
    cache_key = f"vr_portfolio_{vr_code}_{date.today().isoformat()}.html"
    cache_file = CACHE_DIR / cache_key

    if cache_file.exists():
        soup = BeautifulSoup(cache_file.read_text(encoding="utf-8"), "html.parser")
    else:
        _vr_throttle()
        url = f"{VR_BASE}/funds/{vr_code}/portfolio/"
        try:
            r = requests.get(url, timeout=30, headers={"User-Agent": USER_AGENT})
            r.raise_for_status()
            cache_file.write_text(r.text, encoding="utf-8")
            soup = BeautifulSoup(r.text, "html.parser")
        except requests.RequestException as e:
            log.warning(f"Portfolio fetch failed for {vr_code}: {e}")
            return {"holdings": [], "sectors": [], "factsheet_date": None}

    factsheet_date = _extract_factsheet_date(soup)
    holdings = _extract_holdings(soup)
    sectors = _extract_sectors(soup)

    return {
        "factsheet_date": factsheet_date or date.today().replace(day=1).isoformat(),
        "holdings": holdings,
        "sectors": sectors,
    }


def _extract_factsheet_date(soup: BeautifulSoup) -> Optional[str]:
    """VR usually shows 'As on DD-MMM-YYYY' near the portfolio header."""
    text = soup.get_text(" ", strip=True)
    m = re.search(r"[Aa]s on\s+(\d{1,2}[-\s][A-Za-z]{3}[-\s]\d{2,4})", text)
    if not m:
        return None
    raw = m.group(1).replace(" ", "-")
    for fmt in ("%d-%b-%Y", "%d-%b-%y"):
        try:
            return datetime.strptime(raw, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _extract_holdings(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    """
    Find the holdings table. We try a few strategies because VR's HTML varies:
      1. Look for a heading containing "Equity Holdings" or "Top Holdings"
      2. Find the next <table> after that heading
      3. Parse columns: Stock | Sector | % Assets
    """
    holdings = []
    for header in soup.find_all(["h2", "h3", "h4"]):
        if not re.search(r"holdings", header.get_text(strip=True), re.I):
            continue
        table = header.find_next("table")
        if not table:
            continue
        for row in table.select("tbody tr") or table.select("tr")[1:]:
            cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
            if len(cells) < 2:
                continue
            try:
                pct = float(re.sub(r"[^\d.\-]", "", cells[-1]))
            except ValueError:
                continue
            holdings.append({
                "stock_name": cells[0],
                "sector": cells[1] if len(cells) > 2 else None,
                "asset_type": "Equity",
                "percentage": pct,
            })
        if holdings:
            break
    return holdings


def _extract_sectors(soup: BeautifulSoup) -> List[Dict[str, Any]]:
    sectors = []
    for header in soup.find_all(["h2", "h3", "h4"]):
        if not re.search(r"sector\s*allocation|sector\s*break", header.get_text(strip=True), re.I):
            continue
        table = header.find_next("table")
        if not table:
            continue
        for row in table.select("tbody tr") or table.select("tr")[1:]:
            cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
            if len(cells) < 2:
                continue
            try:
                pct = float(re.sub(r"[^\d.\-]", "", cells[-1]))
            except ValueError:
                continue
            sectors.append({"sector": cells[0], "percentage": pct})
        if sectors:
            break
    return sectors


# =====================================================================
# Orchestration — refresh a single scheme end-to-end
# =====================================================================

def refresh_scheme(scheme_code: str) -> Dict[str, Any]:
    """
    For a single scheme: fetch fund details from VR, fetch latest portfolio,
    save factsheet, update fund managers. Returns a summary.

    Triggers alerts via the database layer for any category/objective/manager change.
    """
    scheme_row = db.get_scheme(scheme_code)
    if not scheme_row:
        return {"error": f"Scheme {scheme_code} not in DB. Import portfolio first."}

    vr_code = scheme_row["vr_code"]
    if not vr_code:
        vr_code = find_vr_code(scheme_row["scheme_name"])
        if not vr_code:
            return {"error": f"Could not find VR code for {scheme_row['scheme_name']}"}

    details = parse_fund_details(vr_code)
    if not details:
        return {"error": "Failed to parse fund details from VR"}

    db.upsert_scheme({
        "scheme_code": scheme_code,
        "vr_code": vr_code,
        "isin_growth": scheme_row["isin_growth"],
        "isin_div": scheme_row["isin_div"],
        "scheme_name": details.get("scheme_name") or scheme_row["scheme_name"],
        "category": details.get("category"),
        "sub_category": details.get("sub_category"),
        "fund_house": details.get("fund_house") or scheme_row["fund_house"],
        "objective": details.get("objective"),
        "benchmark": details.get("benchmark"),
        "expense_ratio": details.get("expense_ratio"),
        "aum": details.get("aum"),
    })

    if details.get("fund_managers"):
        db.update_fund_managers(scheme_code, details["fund_managers"])

    portfolio = parse_portfolio(vr_code)
    factsheet_id = None
    if portfolio["holdings"] or portfolio["sectors"]:
        factsheet_id = db.save_factsheet(
            scheme_code,
            portfolio["factsheet_date"],
            portfolio["holdings"],
            portfolio["sectors"],
            source="valueresearch",
            raw=portfolio,
        )

    return {
        "scheme_code": scheme_code,
        "vr_code": vr_code,
        "details_updated": True,
        "factsheet_id": factsheet_id,
        "holdings_count": len(portfolio["holdings"]),
        "sectors_count": len(portfolio["sectors"]),
        "factsheet_date": portfolio["factsheet_date"],
    }
