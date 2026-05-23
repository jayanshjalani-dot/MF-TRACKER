"""
Black-Scholes Option Chain & Greeks calculator.

All math is vectorised via NumPy.  SciPy provides the normal CDF (N).
"""

import math
import numpy as np
import pandas as pd
from scipy.stats import norm
from scipy.optimize import brentq
from typing import Optional
from loguru import logger

from data_fetcher import DataFetcher
from models import OptionStrikeData


# Risk-free rate — use current RBI repo rate approximate
RISK_FREE_RATE = 0.065

# Trading days per year used for Theta scaling
TRADING_DAYS_PER_YEAR = 252


# ---------- Black-Scholes Core ----------

def _d1(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """d1 term in the Black-Scholes formula."""
    return (math.log(S / K) + (r + 0.5 * sigma**2) * T) / (sigma * math.sqrt(T))


def _d2(S: float, K: float, T: float, r: float, sigma: float) -> float:
    return _d1(S, K, T, r, sigma) - sigma * math.sqrt(T)


def bs_price(S: float, K: float, T: float, r: float, sigma: float, option_type: str) -> float:
    """
    Black-Scholes option price.
    S = spot, K = strike, T = time to expiry in years,
    r = risk-free rate, sigma = implied vol, option_type = 'CE' | 'PE'
    """
    if T <= 0 or sigma <= 0:
        # At or past expiry — return intrinsic value
        intrinsic = max(0.0, S - K) if option_type == "CE" else max(0.0, K - S)
        return intrinsic

    d1 = _d1(S, K, T, r, sigma)
    d2 = _d2(S, K, T, r, sigma)

    if option_type == "CE":
        return S * norm.cdf(d1) - K * math.exp(-r * T) * norm.cdf(d2)
    else:  # PE
        return K * math.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)


def implied_volatility(
    market_price: float,
    S: float,
    K: float,
    T: float,
    r: float,
    option_type: str,
    tol: float = 1e-6,
) -> Optional[float]:
    """
    Solve for IV using Brent's method (bracketed root finder).
    Returns None if the market price is outside arbitrage bounds.
    """
    if T <= 0 or market_price <= 0:
        return None

    # Intrinsic value check — market price below intrinsic means bad data
    intrinsic = max(0.0, S - K) if option_type == "CE" else max(0.0, K - S)
    if market_price < intrinsic - 0.01:
        return None

    def objective(sigma):
        return bs_price(S, K, T, r, sigma, option_type) - market_price

    try:
        # Brent search over reasonable vol range: 1% to 500%
        iv = brentq(objective, 1e-4, 5.0, xtol=tol, maxiter=200)
        return iv
    except (ValueError, RuntimeError):
        return None


# ---------- Greeks ----------

def calculate_greeks(
    S: float, K: float, T: float, r: float, sigma: float, option_type: str
) -> dict:
    """
    Compute all five Greeks.
    Delta: rate of change of option price w.r.t. spot
    Gamma: rate of change of delta w.r.t. spot
    Theta: time decay per calendar day (in rupee terms)
    Vega:  sensitivity to 1% change in IV
    Rho:   sensitivity to 1% change in risk-free rate
    """
    if T <= 0 or sigma <= 0:
        return {"delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0, "rho": 0.0}

    d1 = _d1(S, K, T, r, sigma)
    d2 = _d2(S, K, T, r, sigma)
    nd1 = norm.pdf(d1)  # standard normal PDF at d1

    gamma = nd1 / (S * sigma * math.sqrt(T))
    vega = S * nd1 * math.sqrt(T) / 100  # per 1% vol move

    if option_type == "CE":
        delta = norm.cdf(d1)
        theta = (
            -(S * nd1 * sigma) / (2 * math.sqrt(T))
            - r * K * math.exp(-r * T) * norm.cdf(d2)
        ) / 365
        rho = K * T * math.exp(-r * T) * norm.cdf(d2) / 100
    else:
        delta = norm.cdf(d1) - 1
        theta = (
            -(S * nd1 * sigma) / (2 * math.sqrt(T))
            + r * K * math.exp(-r * T) * norm.cdf(-d2)
        ) / 365
        rho = -K * T * math.exp(-r * T) * norm.cdf(-d2) / 100

    return {
        "delta": round(delta, 4),
        "gamma": round(gamma, 6),
        "theta": round(theta, 4),
        "vega": round(vega, 4),
        "rho": round(rho, 4),
    }


# ---------- Option Chain Builder ----------

class OptionChainEngine:
    """
    Builds the full Option Chain with live IV and Greeks.
    Consumes snapshot data from DataFetcher and enriches each strike.
    """

    def __init__(self, data_fetcher: DataFetcher):
        self.fetcher = data_fetcher

    def _time_to_expiry(self, expiry_str: str) -> float:
        """Convert 'DDMMMYYYY' expiry string to fraction of a year."""
        import datetime
        formats = ["%d%b%Y", "%d-%b-%Y", "%Y-%m-%d"]
        expiry_dt = None
        for fmt in formats:
            try:
                expiry_dt = datetime.datetime.strptime(expiry_str.strip(), fmt).date()
                break
            except ValueError:
                continue
        if expiry_dt is None:
            return 0.0
        today = datetime.date.today()
        days = (expiry_dt - today).days
        return max(days / 365.0, 0.0)

    def build_chain(
        self,
        underlying: str = "NIFTY",
        expiry_filter: Optional[str] = None,
        strikes_around_atm: int = 10,
    ) -> list[OptionStrikeData]:
        """
        Fetch live option chain data and compute IV + Greeks for every strike.

        strikes_around_atm: how many strikes above/below ATM to include
        expiry_filter: if None, uses the nearest weekly/monthly expiry
        """
        # Get spot price
        spot = (
            self.fetcher.get_nifty_spot()
            if underlying.upper() == "NIFTY"
            else self.fetcher.get_banknifty_spot()
        )

        # Get options from instrument master
        options = self.fetcher.get_nfo_options(underlying)
        if options.empty:
            logger.warning(f"No options found for {underlying}")
            return []

        # Select the nearest expiry if not specified
        options["expiry_dt"] = pd.to_datetime(options["expiry"], format="%d%b%Y", errors="coerce")
        options = options.dropna(subset=["expiry_dt"])
        available_expiries = sorted(options["expiry_dt"].unique())

        if expiry_filter:
            selected_expiry = pd.to_datetime(expiry_filter)
        else:
            import datetime
            today = pd.Timestamp.today().normalize()
            future_expiries = [e for e in available_expiries if e >= today]
            if not future_expiries:
                return []
            selected_expiry = future_expiries[0]

        chain_df = options[options["expiry_dt"] == selected_expiry].copy()
        chain_df["strike"] = pd.to_numeric(chain_df["strike"], errors="coerce")

        # ATM strike = closest strike to spot
        strikes = sorted(chain_df["strike"].dropna().unique())
        if not strikes:
            return []
        atm = min(strikes, key=lambda x: abs(x - spot))
        atm_idx = strikes.index(atm)
        selected_strikes = strikes[
            max(0, atm_idx - strikes_around_atm): atm_idx + strikes_around_atm + 1
        ]

        T = self._time_to_expiry(selected_expiry.strftime("%d%b%Y"))
        r = RISK_FREE_RATE
        result = []

        for strike in selected_strikes:
            ce_row = chain_df[
                (chain_df["strike"] == strike) & (chain_df["optiontype"] == "CE")
            ]
            pe_row = chain_df[
                (chain_df["strike"] == strike) & (chain_df["optiontype"] == "PE")
            ]

            # Try to get live LTP from Angel One
            ce_ltp = self._fetch_ltp(ce_row)
            pe_ltp = self._fetch_ltp(pe_row)

            # Compute IV and Greeks for CE
            ce_iv = implied_volatility(ce_ltp or 0, spot, strike, T, r, "CE") if ce_ltp else None
            ce_greeks = calculate_greeks(spot, strike, T, r, ce_iv or 0.18, "CE") if ce_iv else {}

            # Compute IV and Greeks for PE
            pe_iv = implied_volatility(pe_ltp or 0, spot, strike, T, r, "PE") if pe_ltp else None
            pe_greeks = calculate_greeks(spot, strike, T, r, pe_iv or 0.18, "PE") if pe_iv else {}

            ce_oi = int(ce_row["lotsize"].iloc[0]) if not ce_row.empty else None
            pe_oi = int(pe_row["lotsize"].iloc[0]) if not pe_row.empty else None

            pcr = round(pe_oi / ce_oi, 4) if (ce_oi and pe_oi and ce_oi > 0) else None

            result.append(
                OptionStrikeData(
                    strike=strike,
                    expiry=selected_expiry.strftime("%d-%b-%Y"),
                    ce_ltp=ce_ltp,
                    ce_oi=ce_oi,
                    ce_iv=round(ce_iv * 100, 2) if ce_iv else None,
                    ce_delta=ce_greeks.get("delta"),
                    ce_gamma=ce_greeks.get("gamma"),
                    ce_theta=ce_greeks.get("theta"),
                    ce_vega=ce_greeks.get("vega"),
                    pe_ltp=pe_ltp,
                    pe_oi=pe_oi,
                    pe_iv=round(pe_iv * 100, 2) if pe_iv else None,
                    pe_delta=pe_greeks.get("delta"),
                    pe_gamma=pe_greeks.get("gamma"),
                    pe_theta=pe_greeks.get("theta"),
                    pe_vega=pe_greeks.get("vega"),
                    pcr=pcr,
                )
            )

        return result

    def _fetch_ltp(self, row: pd.DataFrame) -> Optional[float]:
        if row.empty:
            return None
        try:
            token = str(row.iloc[0]["token"])
            symbol = row.iloc[0]["tradingsymbol"]
            resp = self.fetcher.get_ltp("NFO", symbol, token)
            return float(resp["data"]["ltp"])
        except Exception as e:
            logger.debug(f"LTP fetch skipped: {e}")
            return None

    # ---------- Max Pain ----------

    @staticmethod
    def calculate_max_pain(chain: list[OptionStrikeData]) -> float:
        """
        Max pain = the strike where total option premium (CE+PE) would expire worthless
        at maximum pain to option buyers.  Used as a gravitational reference for expiry.
        """
        min_pain = float("inf")
        max_pain_strike = 0.0

        strikes = [s.strike for s in chain]
        ce_ois = {s.strike: s.ce_oi or 0 for s in chain}
        pe_ois = {s.strike: s.pe_oi or 0 for s in chain}

        for pin in strikes:
            total_pain = 0.0
            for k in strikes:
                # CE writers lose when spot (pin) is above strike
                if pin > k:
                    total_pain += ce_ois[k] * (pin - k)
                # PE writers lose when spot (pin) is below strike
                if pin < k:
                    total_pain += pe_ois[k] * (k - pin)
            if total_pain < min_pain:
                min_pain = total_pain
                max_pain_strike = pin

        return max_pain_strike

    # ---------- PCR ----------

    @staticmethod
    def calculate_pcr(chain: list[OptionStrikeData]) -> float:
        """
        Put-Call Ratio by Open Interest.
        PCR > 1.2  → overly bearish (contrarian bullish signal)
        PCR < 0.7  → overly bullish (contrarian bearish signal)
        """
        total_ce_oi = sum(s.ce_oi or 0 for s in chain)
        total_pe_oi = sum(s.pe_oi or 0 for s in chain)
        if total_ce_oi == 0:
            return 0.0
        return round(total_pe_oi / total_ce_oi, 4)

    # ---------- Strategy Payoff ----------

    @staticmethod
    def straddle_payoff(
        spot_range: np.ndarray,
        strike: float,
        ce_premium: float,
        pe_premium: float,
        lots: int = 1,
        lot_size: int = 50,
    ) -> dict:
        """ATM Straddle sell payoff profile."""
        net_credit = (ce_premium + pe_premium) * lot_size * lots
        pnl = []
        for S in spot_range:
            ce_loss = max(0, S - strike)
            pe_loss = max(0, strike - S)
            pnl.append(net_credit - (ce_loss + pe_loss) * lot_size * lots)
        return {"spot": spot_range.tolist(), "pnl": pnl, "max_profit": net_credit}

    @staticmethod
    def iron_condor_payoff(
        spot_range: np.ndarray,
        lower_pe_buy: float,
        lower_pe_sell: float,
        upper_ce_sell: float,
        upper_ce_buy: float,
        pe_buy_premium: float,
        pe_sell_premium: float,
        ce_sell_premium: float,
        ce_buy_premium: float,
        lots: int = 1,
        lot_size: int = 50,
    ) -> dict:
        """Iron Condor payoff profile — limited risk, limited reward."""
        net_credit = (pe_sell_premium - pe_buy_premium + ce_sell_premium - ce_buy_premium) * lot_size * lots
        pnl = []
        for S in spot_range:
            pe_spread_loss = max(0, lower_pe_sell - S) - max(0, lower_pe_buy - S)
            ce_spread_loss = max(0, S - upper_ce_sell) - max(0, S - upper_ce_buy)
            pnl.append(net_credit - (pe_spread_loss + ce_spread_loss) * lot_size * lots)
        return {
            "spot": spot_range.tolist(),
            "pnl": pnl,
            "max_profit": net_credit,
            "max_loss": (
                (lower_pe_sell - lower_pe_buy) - (pe_sell_premium - pe_buy_premium)
            ) * lot_size * lots,
        }
