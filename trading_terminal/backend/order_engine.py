"""
Order Execution Engine — wraps Angel One order placement with safety rails.

Real money is on the line.  Rules:
- Log every order attempt and result with a timestamp.
- On API error: log the exact Angel One error code, do NOT retry blindly.
- Partial fills are detected and reported but NOT automatically re-submitted.
- The caller decides whether to chase a partial fill.
"""

import time
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

from broker import AngelOneBroker
from models import (
    PlaceOrderRequest,
    ModifyOrderRequest,
    CancelOrderRequest,
)

# How long to wait between the place-order call and the status check
ORDER_STATUS_DELAY_SECONDS = 1.0

# Angel One error codes that are safe to retry (transient network / rate-limit)
RETRYABLE_ERROR_CODES = {"AG8003", "AG8004", "AG8005", "429"}


class AngelOneError(Exception):
    """Raised when Angel One returns a non-retryable order error."""

    def __init__(self, code: str, message: str):
        super().__init__(f"[{code}] {message}")
        self.code = code


class OrderEngine:
    """Executes, modifies, and cancels orders via the Angel One SmartAPI."""

    def __init__(self, broker: AngelOneBroker):
        self.broker = broker

    # ---------- Place ----------

    def place_order(self, req: PlaceOrderRequest) -> dict:
        """
        Place a new order.  Returns Angel One's response dict.
        Raises AngelOneError on any non-retryable failure.
        """
        self.broker._require_session()
        payload = req.model_dump()

        logger.info(
            f"PLACE ORDER | {req.transactiontype.value} {req.quantity} "
            f"{req.tradingsymbol} @ {req.ordertype.value}"
        )

        resp = self._safe_place(payload)
        order_id = resp.get("data", {}).get("orderid", "")
        logger.info(f"Order accepted | order_id={order_id}")

        # Brief pause, then fetch confirmation from orderbook
        time.sleep(ORDER_STATUS_DELAY_SECONDS)
        return self._check_order_status(order_id, req.tradingsymbol)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(2),
        retry=retry_if_exception_type(ConnectionError),
        reraise=True,
    )
    def _safe_place(self, payload: dict) -> dict:
        """Network-retry wrapper — only for transient connection errors."""
        resp = self.broker.smart_api.placeOrder(payload)
        if not resp.get("status"):
            msg = resp.get("message", "unknown")
            code = resp.get("errorcode", "UNKNOWN")
            logger.error(f"Place order failed | code={code} | msg={msg} | payload={payload}")
            if code in RETRYABLE_ERROR_CODES:
                raise ConnectionError(f"Retryable error {code}: {msg}")
            raise AngelOneError(code, msg)
        return resp

    # ---------- Modify ----------

    def modify_order(self, req: ModifyOrderRequest) -> dict:
        """Modify price / quantity of an open order."""
        self.broker._require_session()
        payload = req.model_dump()

        logger.info(f"MODIFY ORDER | order_id={req.orderid} | new_qty={req.quantity} | new_price={req.price}")

        resp = self.broker.smart_api.modifyOrder(payload)
        if not resp.get("status"):
            code = resp.get("errorcode", "UNKNOWN")
            msg = resp.get("message", "unknown")
            logger.error(f"Modify order failed | code={code} | msg={msg}")
            raise AngelOneError(code, msg)

        logger.info(f"Order modified | order_id={req.orderid}")
        return resp

    # ---------- Cancel ----------

    def cancel_order(self, req: CancelOrderRequest) -> dict:
        """Cancel an open order."""
        self.broker._require_session()
        logger.info(f"CANCEL ORDER | order_id={req.orderid}")

        resp = self.broker.smart_api.cancelOrder(req.orderid, req.variety.value)
        if not resp.get("status"):
            code = resp.get("errorcode", "UNKNOWN")
            msg = resp.get("message", "unknown")
            logger.error(f"Cancel order failed | code={code} | msg={msg}")
            raise AngelOneError(code, msg)

        logger.info(f"Order cancelled | order_id={req.orderid}")
        return resp

    # ---------- Status / Book ----------

    def get_order_book(self) -> list[dict]:
        """Return today's order book."""
        self.broker._require_session()
        resp = self.broker.smart_api.orderBook()
        if not resp.get("status"):
            raise RuntimeError(f"Order book fetch failed: {resp.get('message')}")
        return resp.get("data") or []

    def get_trade_book(self) -> list[dict]:
        """Return today's executed trades."""
        self.broker._require_session()
        resp = self.broker.smart_api.tradeBook()
        if not resp.get("status"):
            raise RuntimeError(f"Trade book fetch failed: {resp.get('message')}")
        return resp.get("data") or []

    def get_positions(self) -> list[dict]:
        """Return current open positions (net + day)."""
        self.broker._require_session()
        resp = self.broker.smart_api.position()
        if not resp.get("status"):
            raise RuntimeError(f"Positions fetch failed: {resp.get('message')}")
        return resp.get("data") or []

    def get_holdings(self) -> list[dict]:
        """Return delivery holdings."""
        self.broker._require_session()
        resp = self.broker.smart_api.holding()
        if not resp.get("status"):
            raise RuntimeError(f"Holdings fetch failed: {resp.get('message')}")
        return resp.get("data") or []

    # ---------- Internal ----------

    def _check_order_status(self, order_id: str, symbol: str) -> dict:
        """Fetch the specific order from the order book and log its status."""
        book = self.get_order_book()
        for order in book:
            if order.get("orderid") == order_id:
                status = order.get("status", "UNKNOWN")
                filled = order.get("filledshares", 0)
                total = order.get("quantity", 0)

                if status == "complete":
                    logger.info(f"Order FILLED | {symbol} | qty={filled}")
                elif status == "open":
                    logger.info(f"Order PENDING | {symbol} | filled={filled}/{total}")
                elif filled and int(filled) < int(total):
                    logger.warning(
                        f"PARTIAL FILL | {symbol} | filled={filled}/{total} — "
                        f"manual intervention may be required"
                    )
                else:
                    logger.warning(f"Order status={status} | {symbol}")

                return order

        logger.warning(f"Order {order_id} not found in order book after placement")
        return {"orderid": order_id, "status": "not_found"}
