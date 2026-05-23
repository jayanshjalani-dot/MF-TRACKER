"""
Pydantic models for all API request/response schemas.
"""

from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field


# ---------- Enums ----------

class Exchange(str, Enum):
    NSE = "NSE"
    BSE = "BSE"
    NFO = "NFO"


class TransactionType(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOPLOSS_LIMIT = "STOPLOSS_LIMIT"
    STOPLOSS_MARKET = "STOPLOSS_MARKET"


class ProductType(str, Enum):
    DELIVERY = "DELIVERY"
    CARRYFORWARD = "CARRYFORWARD"
    MARGIN = "MARGIN"
    INTRADAY = "INTRADAY"
    BO = "BO"


class Variety(str, Enum):
    NORMAL = "NORMAL"
    STOPLOSS = "STOPLOSS"
    AMO = "AMO"
    ROBO = "ROBO"


class OptionType(str, Enum):
    CE = "CE"
    PE = "PE"


# ---------- Order Models ----------

class PlaceOrderRequest(BaseModel):
    variety: Variety = Variety.NORMAL
    tradingsymbol: str = Field(..., description="e.g. NIFTY23JAN18000CE")
    symboltoken: str = Field(..., description="Angel One instrument token")
    transactiontype: TransactionType
    exchange: Exchange
    ordertype: OrderType = OrderType.MARKET
    producttype: ProductType = ProductType.INTRADAY
    duration: str = "DAY"
    price: float = 0.0
    squareoff: float = 0.0
    stoploss: float = 0.0
    quantity: int = Field(..., gt=0)
    triggerprice: float = 0.0


class ModifyOrderRequest(BaseModel):
    orderid: str
    variety: Variety = Variety.NORMAL
    ordertype: OrderType
    producttype: ProductType
    duration: str = "DAY"
    price: float = 0.0
    quantity: int = Field(..., gt=0)
    tradingsymbol: str
    symboltoken: str
    exchange: Exchange


class CancelOrderRequest(BaseModel):
    orderid: str
    variety: Variety = Variety.NORMAL


# ---------- Option Chain Models ----------

class OptionStrikeData(BaseModel):
    strike: float
    expiry: str
    ce_ltp: Optional[float] = None
    ce_oi: Optional[int] = None
    ce_volume: Optional[int] = None
    ce_iv: Optional[float] = None
    ce_delta: Optional[float] = None
    ce_gamma: Optional[float] = None
    ce_theta: Optional[float] = None
    ce_vega: Optional[float] = None
    pe_ltp: Optional[float] = None
    pe_oi: Optional[int] = None
    pe_volume: Optional[int] = None
    pe_iv: Optional[float] = None
    pe_delta: Optional[float] = None
    pe_gamma: Optional[float] = None
    pe_theta: Optional[float] = None
    pe_vega: Optional[float] = None
    pcr: Optional[float] = None


# ---------- Backtesting Models ----------

class BacktestRequest(BaseModel):
    symbol: str = "NIFTY"
    exchange: str = "NSE"
    symbol_token: str
    interval: str = "ONE_DAY"
    from_date: str = Field(..., description="YYYY-MM-DD HH:MM")
    to_date: str = Field(..., description="YYYY-MM-DD HH:MM")
    strategy: str = "MA_CROSSOVER"
    fast_period: int = 10
    slow_period: int = 20


class BacktestResult(BaseModel):
    total_trades: int
    win_rate: float
    total_pnl: float
    max_drawdown: float
    sharpe_ratio: float
    profit_factor: float
    trades: list


# ---------- Sentiment Models ----------

class SentimentResult(BaseModel):
    score: float = Field(..., description="-1.0 (very bearish) to +1.0 (very bullish)")
    label: str
    headlines: list[dict]
    trade_blocked: bool = Field(..., description="True if score is extreme enough to block algo trades")
