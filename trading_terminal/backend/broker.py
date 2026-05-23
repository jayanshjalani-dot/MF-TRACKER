"""
Angel One SmartAPI broker integration.
Handles auth, session management, and TOTP.
"""

import os
import pyotp
from SmartApi import SmartConnect
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log
from loguru import logger

load_dotenv()


class AngelOneBroker:
    """
    Manages Angel One SmartAPI session lifecycle.
    Credentials are loaded exclusively from environment variables.
    """

    def __init__(self):
        self.api_key = os.environ["ANGEL_API_KEY"]
        self.client_id = os.environ["ANGEL_CLIENT_ID"]
        self.pin = os.environ["ANGEL_PIN"]
        self.totp_secret = os.environ["ANGEL_TOTP_SECRET"]
        self.smart_api: SmartConnect | None = None
        self.auth_token: str | None = None
        self.feed_token: str | None = None
        self.refresh_token: str | None = None

    # ---------- Authentication ----------

    def _generate_totp(self) -> str:
        """Generate current TOTP using the stored base-32 secret."""
        return pyotp.TOTP(self.totp_secret).now()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def login(self) -> dict:
        """
        Authenticate with Angel One.
        Returns the raw session data dict on success.
        Raises on repeated failure — never silently swallows auth errors.
        """
        self.smart_api = SmartConnect(api_key=self.api_key)
        totp = self._generate_totp()

        data = self.smart_api.generateSession(
            clientCode=self.client_id,
            password=self.pin,
            totp=totp,
        )

        if not data.get("status"):
            # Angel One returns status=False with a message on failure
            raise RuntimeError(f"Angel One login rejected: {data.get('message', 'unknown error')}")

        self.auth_token = data["data"]["jwtToken"]
        self.refresh_token = data["data"]["refreshToken"]
        self.feed_token = self.smart_api.getfeedToken()

        logger.info(f"Angel One session established for client {self.client_id}")
        return data

    def get_profile(self) -> dict:
        self._require_session()
        return self.smart_api.getProfile(self.refresh_token)

    def renew_token(self) -> dict:
        """Refresh the JWT before it expires (Angel One tokens last ~1 day)."""
        self._require_session()
        data = self.smart_api.generateToken(self.refresh_token)
        if not data.get("status"):
            raise RuntimeError(f"Token renewal failed: {data.get('message')}")
        self.auth_token = data["data"]["jwtToken"]
        self.feed_token = data["data"]["feedToken"]
        logger.info("JWT refreshed successfully")
        return data

    def logout(self):
        if self.smart_api:
            try:
                self.smart_api.terminateSession(self.client_id)
                logger.info("Angel One session terminated")
            except Exception as e:
                logger.warning(f"Logout error (non-fatal): {e}")
        self.smart_api = None
        self.auth_token = None
        self.feed_token = None
        self.refresh_token = None

    def is_logged_in(self) -> bool:
        return self.smart_api is not None and self.auth_token is not None

    # ---------- Internal ----------

    def _require_session(self):
        if not self.is_logged_in():
            raise RuntimeError("Not logged in. Call broker.login() first.")
