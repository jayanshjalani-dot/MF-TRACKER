"""
NSE/BSE Trading Terminal — FastAPI Backend
All routes are prefixed with /api/v1.
"""

import asyncio
import json
import os
from contextlib import asynccontextmanager
from typing import Optional

import numpy as np
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from loguru import logger

from broker import AngelOneBroker
from data_fetcher import DataFetcher
from option_chain import OptionChainEngine
from order_engine import OrderEngine, AngelOneError
from sentiment import SentimentEngine
from backtester import Backtester, STRATEGY_MAP
from models import (
    PlaceOrderRequest,
    ModifyOrderRequest,
    CancelOrderRequest,
    BacktestRequest,
)
from ws_handler import TickStreamManager

load_dotenv()


# ---------- Global State ----------

broker = AngelOneBroker()
fetcher = DataFetcher(broker)
chain_engine = OptionChainEngine(fetcher)
order_engine = OrderEngine(broker)
sentiment_engine = SentimentEngine()
backtester = Backtester(fetcher)
tick_manager: Optional[TickStreamManager] = None


# ---------- Lifespan ----------

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Login on startup, logout on shutdown."""
    global tick_manager
    try:
        broker.login()
        logger.info("Broker session started")

        # Prefetch instrument master in the background
        asyncio.get_event_loop().run_in_executor(None, fetcher.fetch_instrument_list)

        # Start WebSocket streaming for Nifty + BankNifty spot
        tick_manager = TickStreamManager(broker)
        nifty_token = "99926000"
        banknifty_token = "99926009"
        tick_manager.subscribe([("NSE", nifty_token), ("NSE", banknifty_token)])
        tick_manager.start()

    except Exception as e:
        logger.error(f"Startup error: {e} — running without live broker session")

    yield

    broker.logout()
    if tick_manager:
        tick_manager.stop()
    logger.info("Broker session closed")


# ---------- App ----------

app = FastAPI(
    title="NSE/BSE Institutional Trading Terminal",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "http://localhost:3000").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------- Health ----------

@app.get("/health")
def health():
    return {
        "status": "ok",
        "broker_logged_in": broker.is_logged_in(),
        "ws_running": tick_manager is not None,
    }


# ---------- Auth ----------

@app.post("/api/v1/auth/login")
def login():
    """(Re)login to Angel One. Useful if session expired."""
    try:
        data = broker.login()
        return {"status": "ok", "client": broker.client_id}
    except Exception as e:
        raise HTTPException(status_code=401, detail=str(e))


@app.get("/api/v1/auth/profile")
def get_profile():
    try:
        return broker.get_profile()
    except RuntimeError as e:
        raise HTTPException(status_code=401, detail=str(e))


# ---------- Instruments ----------

@app.get("/api/v1/instruments/search")
def search_instruments(q: str, exchange: Optional[str] = None):
    """Search for instruments by trading symbol."""
    try:
        df = fetcher.search_symbol(q, exchange)
        return df[["token", "tradingsymbol", "name", "exch_seg", "lotsize", "expiry"]].to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/instruments/ltp")
def get_ltp(exchange: str, symbol: str, token: str):
    """Get last traded price for a single instrument."""
    try:
        return fetcher.get_ltp(exchange, symbol, token)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/instruments/nifty-spot")
def get_nifty_spot():
    try:
        return {"underlying": "NIFTY", "spot": fetcher.get_nifty_spot()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/instruments/banknifty-spot")
def get_banknifty_spot():
    try:
        return {"underlying": "BANKNIFTY", "spot": fetcher.get_banknifty_spot()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------- Historical Data ----------

@app.get("/api/v1/historical")
def get_historical(
    token: str,
    exchange: str = "NSE",
    interval: str = "ONE_DAY",
    from_date: str = Query(..., description="YYYY-MM-DD HH:MM"),
    to_date: str = Query(..., description="YYYY-MM-DD HH:MM"),
):
    """Fetch OHLCV candles for charting."""
    try:
        df = fetcher.get_historical_data(token, exchange, interval, from_date, to_date)
        df_reset = df.reset_index()
        df_reset["timestamp"] = df_reset["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S")
        return df_reset.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------- Option Chain ----------

@app.get("/api/v1/option-chain/{underlying}")
def get_option_chain(
    underlying: str,
    expiry: Optional[str] = None,
    strikes_around_atm: int = 10,
):
    """
    Full option chain with live IV and Greeks.
    underlying: NIFTY or BANKNIFTY
    """
    try:
        chain = chain_engine.build_chain(
            underlying=underlying.upper(),
            expiry_filter=expiry,
            strikes_around_atm=strikes_around_atm,
        )
        max_pain = chain_engine.calculate_max_pain(chain)
        pcr = chain_engine.calculate_pcr(chain)
        return {
            "underlying": underlying.upper(),
            "max_pain": max_pain,
            "pcr": pcr,
            "chain": [s.model_dump() for s in chain],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/strategy/payoff")
def get_strategy_payoff(
    strategy: str,
    strike: float,
    spot: float,
    ce_premium: float = 100.0,
    pe_premium: float = 100.0,
    lots: int = 1,
    lot_size: int = 50,
):
    """
    Generate strategy payoff curve data.
    strategy: STRADDLE | IRON_CONDOR
    """
    spot_range = np.linspace(spot * 0.85, spot * 1.15, 200)
    try:
        if strategy.upper() == "STRADDLE":
            payoff = chain_engine.straddle_payoff(spot_range, strike, ce_premium, pe_premium, lots, lot_size)
        else:
            raise HTTPException(status_code=400, detail=f"Strategy {strategy} not yet supported via this endpoint. Use STRADDLE.")
        return payoff
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------- Orders ----------

@app.post("/api/v1/orders/place")
def place_order(req: PlaceOrderRequest):
    """Place a new order. Validates sentiment before executing."""
    # Safety check: block if macro sentiment is extreme
    sentiment = sentiment_engine.get_sentiment()
    if sentiment.get("trade_blocked"):
        raise HTTPException(
            status_code=403,
            detail=f"Trade blocked: extreme market sentiment ({sentiment['label']}). "
                   f"Override in the terminal settings if intentional.",
        )
    try:
        result = order_engine.place_order(req)
        return result
    except AngelOneError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.put("/api/v1/orders/modify")
def modify_order(req: ModifyOrderRequest):
    try:
        return order_engine.modify_order(req)
    except AngelOneError as e:
        raise HTTPException(status_code=422, detail=str(e))


@app.delete("/api/v1/orders/cancel")
def cancel_order(req: CancelOrderRequest):
    try:
        return order_engine.cancel_order(req)
    except AngelOneError as e:
        raise HTTPException(status_code=422, detail=str(e))


@app.get("/api/v1/orders/book")
def get_order_book():
    try:
        return order_engine.get_order_book()
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/orders/trades")
def get_trade_book():
    try:
        return order_engine.get_trade_book()
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/portfolio/positions")
def get_positions():
    try:
        return order_engine.get_positions()
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/portfolio/holdings")
def get_holdings():
    try:
        return order_engine.get_holdings()
    except RuntimeError as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------- Sentiment ----------

@app.get("/api/v1/sentiment")
def get_sentiment(refresh: bool = False):
    """Current macro sentiment score and top headlines."""
    return sentiment_engine.get_sentiment(force_refresh=refresh)


# ---------- Backtesting ----------

@app.post("/api/v1/backtest")
def run_backtest(req: BacktestRequest):
    """
    Run a vectorised backtest.
    Strategies: MA_CROSSOVER, SUPERTREND
    """
    if req.strategy.upper() not in STRATEGY_MAP:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown strategy '{req.strategy}'. Available: {list(STRATEGY_MAP.keys())}",
        )
    try:
        result = backtester.run_from_api(
            symbol_token=req.symbol_token,
            exchange=req.exchange,
            interval=req.interval,
            from_date=req.from_date,
            to_date=req.to_date,
            strategy=req.strategy,
            fast_period=req.fast_period,
            slow_period=req.slow_period,
        )
        return {
            "total_trades": result.total_trades,
            "win_rate": result.win_rate,
            "total_pnl": result.total_pnl,
            "total_pnl_pct": result.total_pnl_pct,
            "max_drawdown": result.max_drawdown,
            "max_drawdown_pct": result.max_drawdown_pct,
            "sharpe_ratio": result.sharpe_ratio,
            "profit_factor": result.profit_factor,
            "avg_win": result.avg_win,
            "avg_loss": result.avg_loss,
            "best_trade": result.best_trade,
            "worst_trade": result.worst_trade,
            "trades": [
                {
                    "entry_date": t.entry_date,
                    "exit_date": t.exit_date,
                    "entry_price": t.entry_price,
                    "exit_price": t.exit_price,
                    "direction": t.direction,
                    "pnl": t.pnl,
                    "pnl_pct": t.pnl_pct,
                    "bars_held": t.bars_held,
                }
                for t in result.trades
            ],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------- Server-Sent Events (live ticks) ----------

@app.get("/api/v1/stream/ticks")
async def stream_ticks():
    """
    SSE endpoint that streams live tick data to the frontend.
    The frontend EventSource connects here and receives every tick
    pushed from the Angel One WebSocket.
    """
    async def event_generator():
        while True:
            try:
                tick = await asyncio.wait_for(
                    tick_manager.tick_queue.get() if tick_manager else asyncio.sleep(1),
                    timeout=30.0,
                )
                yield f"data: {json.dumps(tick)}\n\n"
            except asyncio.TimeoutError:
                yield ": keepalive\n\n"  # prevent proxy timeout
            except Exception:
                break

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


# ---------- WebSocket subscription management ----------

@app.post("/api/v1/stream/subscribe")
def subscribe_tokens(tokens: list[dict]):
    """
    Dynamically add tokens to the live feed.
    Body: [{"exchange": "NFO", "token": "43411"}, ...]
    """
    if tick_manager is None:
        raise HTTPException(status_code=503, detail="WebSocket not running")
    pairs = [(t["exchange"], t["token"]) for t in tokens]
    tick_manager.subscribe(pairs)
    return {"subscribed": len(pairs)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=os.getenv("BACKEND_HOST", "0.0.0.0"),
        port=int(os.getenv("BACKEND_PORT", 8000)),
        reload=True,
        log_level="info",
    )
