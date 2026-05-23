"""
Market data fetching — instrument master, LTP, and historical OHLCV.
"""

import time
import requests
import pandas as pd
from typing import Optional
from tenacity import retry, stop_after_attempt, wait_exponential
from loguru import logger

from broker import AngelOneBroker

# Angel One hosts the full scrip master as a public JSON file
INSTRUMENT_MASTER_URL = (
    "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
)

# Seconds between consecutive REST calls to stay under the 20 req/s limit
REQUEST_THROTTLE = 0.06


class DataFetcher:
    """Fetches instrument lists, LTP, and historical candles from Angel One."""

    def __init__(self, broker: AngelOneBroker):
        self.broker = broker
        self._df: Optional[pd.DataFrame] = None  # cached instrument master

    # ---------- Instrument Master ----------

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    def fetch_instrument_list(self) -> pd.DataFrame:
        """
        Download the Angel One scrip master (~40 MB JSON).
        Cached in memory for the session to avoid repeat downloads.
        """
        logger.info("Downloading instrument master list...")
        resp = requests.get(INSTRUMENT_MASTER_URL, timeout=60)
        resp.raise_for_status()
        df = pd.DataFrame(resp.json())
        df.columns = df.columns.str.lower()
        self._df = df
        logger.info(f"Instrument master loaded: {len(df)} rows")
        return df

    @property
    def instruments(self) -> pd.DataFrame:
        """Return cached instruments, fetching if needed."""
        if self._df is None:
            self.fetch_instrument_list()
        return self._df

    def get_nse_cash(self) -> pd.DataFrame:
        """NSE EQ segment — cash equities."""
        return self.instruments[self.instruments["exch_seg"] == "NSE"].copy()

    def get_nfo_options(self, underlying: Optional[str] = None) -> pd.DataFrame:
        """
        NFO Options segment.
        Pass underlying='NIFTY' or 'BANKNIFTY' to narrow results.
        """
        nfo = self.instruments[
            (self.instruments["exch_seg"] == "NFO")
            & (self.instruments["instrumenttype"] == "OPTIDX")
        ].copy()
        if underlying:
            nfo = nfo[nfo["name"] == underlying.upper()]
        return nfo

    def get_nfo_futures(self, underlying: Optional[str] = None) -> pd.DataFrame:
        """NFO Futures segment."""
        fut = self.instruments[
            (self.instruments["exch_seg"] == "NFO")
            & (self.instruments["instrumenttype"] == "FUTSTK")
        ].copy()
        if underlying:
            fut = fut[fut["name"] == underlying.upper()]
        return fut

    def get_token(self, symbol: str, exchange: str = "NSE") -> Optional[str]:
        """Resolve a trading symbol to its Angel One instrument token."""
        match = self.instruments[
            (self.instruments["tradingsymbol"] == symbol.upper())
            & (self.instruments["exch_seg"] == exchange.upper())
        ]
        if match.empty:
            return None
        return str(match.iloc[0]["token"])

    def search_symbol(self, query: str, exchange: Optional[str] = None) -> pd.DataFrame:
        """Full-text search on tradingsymbol column."""
        df = self.instruments
        if exchange:
            df = df[df["exch_seg"] == exchange.upper()]
        return df[df["tradingsymbol"].str.contains(query.upper(), na=False)].head(20)

    # ---------- Live Prices ----------

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=5))
    def get_ltp(self, exchange: str, trading_symbol: str, symbol_token: str) -> dict:
        """Last Traded Price for a single instrument."""
        self.broker._require_session()
        time.sleep(REQUEST_THROTTLE)
        return self.broker.smart_api.ltpData(exchange, trading_symbol, symbol_token)

    def get_nifty_spot(self) -> float:
        """Convenience: return the Nifty 50 spot price."""
        token = self.get_token("Nifty 50", "NSE")
        if not token:
            # Fallback token known to be stable
            token = "99926000"
        resp = self.get_ltp("NSE", "Nifty 50", token)
        return float(resp["data"]["ltp"])

    def get_banknifty_spot(self) -> float:
        """Convenience: return the Bank Nifty spot price."""
        token = self.get_token("Nifty Bank", "NSE") or "99926009"
        resp = self.get_ltp("NSE", "Nifty Bank", token)
        return float(resp["data"]["ltp"])

    # ---------- Historical OHLCV ----------

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    def get_historical_data(
        self,
        symbol_token: str,
        exchange: str,
        interval: str,
        from_date: str,
        to_date: str,
    ) -> pd.DataFrame:
        """
        Fetch OHLCV candle data.

        interval options:
            ONE_MINUTE, THREE_MINUTE, FIVE_MINUTE, TEN_MINUTE,
            FIFTEEN_MINUTE, THIRTY_MINUTE, ONE_HOUR, ONE_DAY

        from_date / to_date format: "YYYY-MM-DD HH:MM"
        """
        self.broker._require_session()
        time.sleep(REQUEST_THROTTLE)

        params = {
            "exchange": exchange,
            "symboltoken": symbol_token,
            "interval": interval,
            "fromdate": from_date,
            "todate": to_date,
        }
        resp = self.broker.smart_api.getCandleData(params)

        if not resp.get("status"):
            raise ValueError(f"Historical data error: {resp.get('message', resp)}")

        cols = ["timestamp", "open", "high", "low", "close", "volume"]
        df = pd.DataFrame(resp["data"], columns=cols)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.set_index("timestamp")
        df[["open", "high", "low", "close", "volume"]] = df[
            ["open", "high", "low", "close", "volume"]
        ].apply(pd.to_numeric)
        return df

    # ---------- Option Chain (REST snapshot) ----------

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=8))
    def get_option_chain_snapshot(self, underlying: str = "NIFTY") -> dict:
        """
        Fetch Angel One's Option Greeks / chain snapshot.
        Returns raw API response; option_chain.py handles the math.
        """
        self.broker._require_session()
        time.sleep(REQUEST_THROTTLE)
        resp = self.broker.smart_api.optionGreek(underlying.upper())
        if not resp.get("status"):
            raise ValueError(f"Option chain fetch failed: {resp.get('message')}")
        return resp
