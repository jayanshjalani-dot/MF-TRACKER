"""
WebSocket streaming handler using Angel One SmartWebSocketV2.

Streams tick-by-tick LTP data for selected tokens.
Auto-reconnects on disconnect with exponential backoff.
Publishes ticks to an asyncio.Queue consumed by the FastAPI SSE endpoint.
"""

import asyncio
import json
import threading
import time
from typing import Callable, Optional
from loguru import logger

try:
    from SmartApi.smartWebSocketV2 import SmartWebSocketV2
except ImportError:
    SmartWebSocketV2 = None  # Allow module load without the SDK installed

from broker import AngelOneBroker

# Feed mode constants (Angel One docs)
FEED_MODE_LTP = 1
FEED_MODE_QUOTE = 2
FEED_MODE_SNAP_QUOTE = 3

# Max backoff wait between reconnect attempts
MAX_RECONNECT_WAIT = 60
INITIAL_RECONNECT_WAIT = 2


class TickStreamManager:
    """
    Manages a persistent Angel One WebSocket connection.

    Usage:
        manager = TickStreamManager(broker)
        manager.subscribe([("NSE", "99926000"), ("NFO", "43411")])
        manager.start()
        # Ticks arrive via on_tick callback or the internal queue
    """

    def __init__(
        self,
        broker: AngelOneBroker,
        on_tick: Optional[Callable[[dict], None]] = None,
    ):
        self.broker = broker
        self.on_tick = on_tick
        self._subscriptions: list[dict] = []
        self._ws: Optional[SmartWebSocketV2] = None
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._reconnect_wait = INITIAL_RECONNECT_WAIT

        # asyncio queue for SSE consumers
        self.tick_queue: asyncio.Queue = asyncio.Queue(maxsize=1000)

    # ---------- Public API ----------

    def subscribe(self, tokens: list[tuple[str, str]], mode: int = FEED_MODE_LTP):
        """
        tokens: list of (exchange, token) pairs
                e.g. [("NSE", "99926000"), ("NFO", "43411")]
        mode:   1=LTP, 2=Quote, 3=SnapQuote
        """
        # Group by exchange as Angel One expects
        exchange_map: dict[str, list] = {}
        for exch, tok in tokens:
            exchange_map.setdefault(exch, []).append({"exchangeType": self._exch_code(exch), "tokens": []})

        # Build the canonical subscription payload
        token_list = []
        for exch, tok in tokens:
            token_list.append({"exchangeType": self._exch_code(exch), "tokens": [tok]})

        self._subscriptions = token_list
        self._mode = mode
        logger.info(f"Registered {len(tokens)} tokens for streaming")

    def start(self):
        """Launch the WebSocket in a background daemon thread."""
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="ws-stream")
        self._thread.start()
        logger.info("WebSocket streaming thread started")

    def stop(self):
        """Gracefully stop the WebSocket."""
        self._running = False
        if self._ws:
            try:
                self._ws.close_connection()
            except Exception:
                pass
        logger.info("WebSocket streaming stopped")

    # ---------- Internal ----------

    def _run_loop(self):
        """Reconnect loop — restarts the WebSocket on any disconnect."""
        while self._running:
            try:
                self._connect()
                self._reconnect_wait = INITIAL_RECONNECT_WAIT  # reset on clean connect
            except Exception as e:
                logger.error(f"WebSocket error: {e}")
            if self._running:
                logger.warning(
                    f"WebSocket disconnected. Reconnecting in {self._reconnect_wait}s..."
                )
                time.sleep(self._reconnect_wait)
                # Exponential backoff capped at MAX_RECONNECT_WAIT
                self._reconnect_wait = min(self._reconnect_wait * 2, MAX_RECONNECT_WAIT)

    def _connect(self):
        """Open the SmartWebSocketV2 connection and subscribe."""
        self.broker._require_session()

        if SmartWebSocketV2 is None:
            raise RuntimeError("SmartWebSocketV2 not available — install smartapi-python")

        self._ws = SmartWebSocketV2(
            auth_token=self.broker.auth_token,
            api_key=self.broker.api_key,
            client_code=self.broker.client_id,
            feed_token=self.broker.feed_token,
        )

        self._ws.on_open = self._on_open
        self._ws.on_data = self._on_data
        self._ws.on_error = self._on_error
        self._ws.on_close = self._on_close

        logger.info("Connecting to Angel One WebSocket...")
        self._ws.connect()

    def _on_open(self, ws):
        logger.info("WebSocket connection opened")
        if self._subscriptions:
            self._ws.subscribe(
                correlation_id="terminal",
                mode=self._mode,
                token_list=self._subscriptions,
            )
            logger.info("Subscriptions sent to Angel One feed")

    def _on_data(self, ws, message):
        """
        Incoming tick. Angel One sends binary frames; SmartWebSocketV2 parses them.
        Each tick dict contains: token, ltp, ltq, volume, bid, ask, oi, etc.
        """
        try:
            tick = message if isinstance(message, dict) else json.loads(message)

            # Normalise to a consistent structure
            normalised = {
                "token": tick.get("token"),
                "ltp": tick.get("last_traded_price", tick.get("ltp", 0)) / 100,  # paise → rupees
                "volume": tick.get("volume_trade_for_the_day", tick.get("volume", 0)),
                "oi": tick.get("open_interest", tick.get("oi", 0)),
                "bid": tick.get("best_5_buy_data", [{}])[0].get("price", 0) / 100 if tick.get("best_5_buy_data") else 0,
                "ask": tick.get("best_5_sell_data", [{}])[0].get("price", 0) / 100 if tick.get("best_5_sell_data") else 0,
                "ts": tick.get("exchange_timestamp", ""),
            }

            # Fire user callback if provided
            if self.on_tick:
                self.on_tick(normalised)

            # Non-blocking push to SSE queue; drop oldest if full
            try:
                self.tick_queue.put_nowait(normalised)
            except asyncio.QueueFull:
                self.tick_queue.get_nowait()  # drop oldest
                self.tick_queue.put_nowait(normalised)

        except Exception as e:
            logger.debug(f"Tick parse error: {e}")

    def _on_error(self, ws, error):
        logger.error(f"WebSocket error: {error}")

    def _on_close(self, ws):
        logger.warning("WebSocket connection closed")

    @staticmethod
    def _exch_code(exchange: str) -> int:
        """Map exchange name to Angel One numeric exchange type."""
        mapping = {"NSE": 1, "NFO": 2, "BSE": 3, "BFO": 4, "MCX": 5, "NCDEX": 7}
        return mapping.get(exchange.upper(), 1)
