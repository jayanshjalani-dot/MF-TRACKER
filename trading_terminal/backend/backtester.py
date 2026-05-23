"""
Vectorised Backtesting Engine.

Implements a signal-driven backtester using Pandas.
Strategies are defined as pure functions: DataFrame → Series of signals.

Metrics: CAGR, Sharpe Ratio, Max Drawdown, Win Rate, Profit Factor.
"""

import numpy as np
import pandas as pd
from dataclasses import dataclass, field
from typing import Callable, Optional
from loguru import logger

from data_fetcher import DataFetcher

# ---------- Trade Record ----------

@dataclass
class Trade:
    entry_date: str
    exit_date: str
    entry_price: float
    exit_price: float
    direction: str       # "LONG" or "SHORT"
    pnl: float
    pnl_pct: float
    bars_held: int


# ---------- Result ----------

@dataclass
class BacktestResult:
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    win_rate: float = 0.0
    total_pnl: float = 0.0
    total_pnl_pct: float = 0.0
    max_drawdown: float = 0.0
    max_drawdown_pct: float = 0.0
    sharpe_ratio: float = 0.0
    profit_factor: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    best_trade: float = 0.0
    worst_trade: float = 0.0
    trades: list[Trade] = field(default_factory=list)


# ---------- Strategy Library ----------

def _ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()


def strategy_ma_crossover(df: pd.DataFrame, fast: int = 10, slow: int = 20) -> pd.Series:
    """
    Classic Moving Average Crossover.
    Signal = +1 when fast EMA crosses above slow EMA (buy).
    Signal = -1 when fast EMA crosses below slow EMA (sell/exit).
    Signal = 0  no position.
    """
    fast_ema = _ema(df["close"], fast)
    slow_ema = _ema(df["close"], slow)
    signal = pd.Series(0, index=df.index)
    signal[fast_ema > slow_ema] = 1
    signal[fast_ema < slow_ema] = -1
    return signal


def strategy_supertrend(df: pd.DataFrame, period: int = 7, multiplier: float = 3.0) -> pd.Series:
    """
    Supertrend Indicator.
    Signal = +1 (price above supertrend band) → bullish
    Signal = -1 (price below supertrend band) → bearish

    Used in the mission spec: "Buy ATM Call when Supertrend flips green"
    """
    hl2 = (df["high"] + df["low"]) / 2

    # True Range
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - df["close"].shift()).abs(),
        (df["low"] - df["close"].shift()).abs(),
    ], axis=1).max(axis=1)

    # ATR via Wilder smoothing (equivalent to EMA with span = 2*period - 1)
    atr = tr.ewm(alpha=1 / period, adjust=False).mean()

    upper_band = hl2 + multiplier * atr
    lower_band = hl2 - multiplier * atr

    supertrend = pd.Series(np.nan, index=df.index)
    direction = pd.Series(1, index=df.index)  # 1=bullish, -1=bearish

    for i in range(1, len(df)):
        # Upper band
        upper_band.iloc[i] = (
            upper_band.iloc[i]
            if upper_band.iloc[i] < upper_band.iloc[i - 1] or df["close"].iloc[i - 1] > upper_band.iloc[i - 1]
            else upper_band.iloc[i - 1]
        )
        # Lower band
        lower_band.iloc[i] = (
            lower_band.iloc[i]
            if lower_band.iloc[i] > lower_band.iloc[i - 1] or df["close"].iloc[i - 1] < lower_band.iloc[i - 1]
            else lower_band.iloc[i - 1]
        )
        # Direction
        if supertrend.iloc[i - 1] == upper_band.iloc[i - 1]:
            direction.iloc[i] = -1 if df["close"].iloc[i] > upper_band.iloc[i] else 1
        else:
            direction.iloc[i] = 1 if df["close"].iloc[i] < lower_band.iloc[i] else -1

        supertrend.iloc[i] = lower_band.iloc[i] if direction.iloc[i] == -1 else upper_band.iloc[i]

    signal = pd.Series(0, index=df.index)
    signal[direction == -1] = 1   # Supertrend is green = bullish
    signal[direction == 1] = -1   # Supertrend is red   = bearish
    return signal


def strategy_200ema_filter(
    df: pd.DataFrame,
    base_signal: pd.Series,
    ema_period: int = 200,
) -> pd.Series:
    """
    Apply a 200 EMA trend filter to any base signal.
    Only allow LONG trades when price is above the 200 EMA.
    This matches the spec: "Nifty is above 200 EMA".
    """
    ema200 = _ema(df["close"], ema_period)
    filtered = base_signal.copy()
    # Suppress buy signals when below 200 EMA
    filtered[(base_signal == 1) & (df["close"] < ema200)] = 0
    return filtered


STRATEGY_MAP: dict[str, Callable] = {
    "MA_CROSSOVER": strategy_ma_crossover,
    "SUPERTREND": strategy_supertrend,
}


# ---------- Engine ----------

class Backtester:
    """
    Vectorised backtesting engine.

    The signal series determines position:
      +1 → Long (hold),  -1 → Short (hold),  0 → Flat
    Positions are entered on the next bar's open (realistic execution).
    """

    def __init__(self, data_fetcher: Optional[DataFetcher] = None):
        self.fetcher = data_fetcher

    def run(
        self,
        df: pd.DataFrame,
        strategy_fn: Callable,
        strategy_kwargs: dict = {},
        use_ema_filter: bool = False,
        initial_capital: float = 100_000,
        commission_pct: float = 0.0003,  # 0.03% per side (realistic for F&O)
    ) -> BacktestResult:
        """
        Run a backtest.

        df: OHLCV DataFrame indexed by datetime
        strategy_fn: a strategy function returning a signal Series
        """
        df = df.copy().dropna()
        if len(df) < 50:
            raise ValueError("Need at least 50 bars to backtest meaningfully")

        # Generate signals
        signals = strategy_fn(df, **strategy_kwargs)

        if use_ema_filter:
            signals = strategy_200ema_filter(df, signals)

        # Shift signal by 1 bar — enter on next open (avoid look-ahead bias)
        position = signals.shift(1).fillna(0)

        return self._simulate(df, position, initial_capital, commission_pct)

    def run_from_api(
        self,
        symbol_token: str,
        exchange: str,
        interval: str,
        from_date: str,
        to_date: str,
        strategy: str = "MA_CROSSOVER",
        fast_period: int = 10,
        slow_period: int = 20,
        use_ema_filter: bool = False,
    ) -> BacktestResult:
        """Fetch data and run backtest in one call."""
        if self.fetcher is None:
            raise RuntimeError("DataFetcher required to run_from_api")

        df = self.fetcher.get_historical_data(
            symbol_token, exchange, interval, from_date, to_date
        )
        fn = STRATEGY_MAP.get(strategy.upper())
        if fn is None:
            raise ValueError(f"Unknown strategy: {strategy}. Available: {list(STRATEGY_MAP)}")

        kwargs = {"fast": fast_period, "slow": slow_period} if strategy.upper() == "MA_CROSSOVER" else {}
        return self.run(df, fn, strategy_kwargs=kwargs, use_ema_filter=use_ema_filter)

    # ---------- Core Simulation ----------

    def _simulate(
        self,
        df: pd.DataFrame,
        position: pd.Series,
        capital: float,
        commission: float,
    ) -> BacktestResult:
        """
        Simulate trades from position signals and compute performance metrics.
        position: +1 Long, -1 Short, 0 Flat.
        """
        trades: list[Trade] = []
        equity = [capital]
        current_pos = 0
        entry_price = 0.0
        entry_idx = 0

        closes = df["close"].values
        opens = df["open"].values
        dates = df.index

        for i in range(1, len(df)):
            signal = int(position.iloc[i])
            prev_pos = current_pos

            # Position change
            if signal != prev_pos:
                # Close existing position
                if prev_pos != 0:
                    exit_price = opens[i]
                    pnl = (exit_price - entry_price) * prev_pos
                    cost = (entry_price + exit_price) * commission
                    net_pnl = pnl - cost
                    equity.append(equity[-1] + net_pnl)
                    trades.append(
                        Trade(
                            entry_date=str(dates[entry_idx]),
                            exit_date=str(dates[i]),
                            entry_price=round(entry_price, 2),
                            exit_price=round(exit_price, 2),
                            direction="LONG" if prev_pos > 0 else "SHORT",
                            pnl=round(net_pnl, 2),
                            pnl_pct=round(net_pnl / (entry_price + 1e-9) * 100, 3),
                            bars_held=i - entry_idx,
                        )
                    )

                # Open new position
                if signal != 0:
                    entry_price = opens[i]
                    entry_idx = i
                current_pos = signal
            else:
                equity.append(equity[-1])

        result = self._calculate_metrics(trades, equity, capital)
        return result

    # ---------- Metrics ----------

    def _calculate_metrics(
        self,
        trades: list[Trade],
        equity: list[float],
        initial_capital: float,
    ) -> BacktestResult:
        res = BacktestResult(trades=trades)
        res.total_trades = len(trades)
        if not trades:
            return res

        pnls = [t.pnl for t in trades]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]

        res.winning_trades = len(wins)
        res.losing_trades = len(losses)
        res.win_rate = round(len(wins) / len(trades) * 100, 2) if trades else 0.0
        res.total_pnl = round(sum(pnls), 2)
        res.total_pnl_pct = round(res.total_pnl / initial_capital * 100, 2)
        res.avg_win = round(np.mean(wins), 2) if wins else 0.0
        res.avg_loss = round(np.mean(losses), 2) if losses else 0.0
        res.best_trade = round(max(pnls), 2)
        res.worst_trade = round(min(pnls), 2)

        # Profit Factor = gross profit / gross loss
        gross_profit = sum(wins)
        gross_loss = abs(sum(losses))
        res.profit_factor = round(gross_profit / gross_loss, 3) if gross_loss > 0 else float("inf")

        # Max Drawdown on equity curve
        eq = np.array(equity)
        peak = np.maximum.accumulate(eq)
        drawdown = peak - eq
        res.max_drawdown = round(float(np.max(drawdown)), 2)
        res.max_drawdown_pct = round(float(np.max(drawdown / peak)) * 100, 2)

        # Sharpe Ratio (daily returns, annualised)
        daily_returns = pd.Series(equity).pct_change().dropna()
        if daily_returns.std() > 0:
            res.sharpe_ratio = round(
                float(daily_returns.mean() / daily_returns.std() * np.sqrt(252)), 3
            )

        return res
