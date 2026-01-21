"""Custom DLT Authentication for Shopee API with HMAC signature and auto token refresh."""

import hashlib
import hmac
import time
from typing import Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import requests
from dlt.common import logger
from dlt.common.configuration.specs import configspec
from dlt.common.pendulum import pendulum
from dlt.sources.helpers.rest_client.auth import AuthConfigBase
from requests import PreparedRequest

from sellpipelines.token_manager import (
    is_token_expired,
    load_tokens_from_state,
    parse_token_expiry,
    save_tokens_to_state,
)

PLATFORM_NAME = "shopee"


@configspec
class ShopeeAuth(AuthConfigBase):
    """Custom authentication for Shopee API with HMAC-SHA256 signature and auto token refresh.

    Shopee requires:
    - partner_id: Application/Partner ID
    - partner_key: Application/Partner Key (secret)
    - shop_id: Shop ID
    - access_token: OAuth access token
    - refresh_token: OAuth refresh token (for auto-refresh)
    - timestamp: Current Unix timestamp
    - sign: HMAC-SHA256 signature

    Token refresh is automatic when the access token expires.
    Tokens are persisted in dlt pipeline state.
    """

    def __init__(
        self,
        partner_id: int,
        partner_key: str,
        shop_id: int,
        access_token: str,
        refresh_token: Optional[str] = None,
        token_expiry_seconds: Optional[int] = None,
        auto_refresh: bool = True,
    ):
        super().__init__()
        self.partner_id = partner_id
        self.partner_key = partner_key
        self.shop_id = shop_id
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.auto_refresh = auto_refresh
        self._host = "https://partner.shopeemobile.com"
        self._state_checked = False

        # Token expiry tracking
        # Shopee tokens expire in 4 hours (14400 seconds) by default
        if token_expiry_seconds:
            self.token_expiry = pendulum.now().add(seconds=token_expiry_seconds)
        else:
            # Default: assume token is valid for 1 hour (to be safe)
            self.token_expiry = pendulum.now().add(hours=1)

    def _check_state_for_tokens(self) -> None:
        """Check pipeline state for stored tokens (only once per session)."""
        if self._state_checked:
            return

        self._state_checked = True

        try:
            stored_tokens = load_tokens_from_state(PLATFORM_NAME)
            if stored_tokens:
                stored_expiry = parse_token_expiry(
                    stored_tokens.get("token_expiry")
                )

                # Use stored tokens if they're newer/valid
                if stored_expiry and not is_token_expired(stored_expiry):
                    self.access_token = stored_tokens["access_token"]
                    self.refresh_token = stored_tokens["refresh_token"]
                    self.token_expiry = stored_expiry
                    logger.info("Using Shopee tokens from pipeline state")
                elif stored_tokens.get("refresh_token"):
                    # Tokens expired but we have refresh token from state
                    self.refresh_token = stored_tokens["refresh_token"]
                    logger.info(
                        "Shopee tokens expired, will refresh using stored refresh token"
                    )
        except Exception as e:
            logger.debug(f"Could not check state for tokens: {e}")

    def _get_timestamp(self) -> int:
        """Get current Unix timestamp."""
        return int(time.time())

    def _generate_signature(self, path: str, timestamp: int) -> str:
        """Generate HMAC-SHA256 signature.

        Base string format: {partner_id}{path}{timestamp}{access_token}{shop_id}
        """
        base_string = (
            f"{self.partner_id}{path}{timestamp}"
            f"{self.access_token}{self.shop_id}"
        )

        signature = hmac.new(
            self.partner_key.encode("utf-8"),
            base_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        return signature

    def _is_token_expired(self) -> bool:
        """Check if the access token is expired or about to expire."""
        return is_token_expired(self.token_expiry)

    def _refresh_access_token(self) -> bool:
        """Refresh the access token using the refresh token.

        Returns:
            True if refresh was successful, False otherwise.
        """
        if not self.refresh_token:
            logger.warning(
                "No refresh token available, cannot refresh access token"
            )
            return False

        logger.info("Refreshing Shopee access token...")

        path = "/api/v2/auth/access_token/get"
        timestamp = self._get_timestamp()

        # Generate signature for token refresh
        # For token refresh, signature is: partner_id + path + timestamp
        base_string = f"{self.partner_id}{path}{timestamp}"
        signature = hmac.new(
            self.partner_key.encode("utf-8"),
            base_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        url = (
            f"{self._host}{path}"
            f"?partner_id={self.partner_id}"
            f"&timestamp={timestamp}"
            f"&sign={signature}"
        )

        body = {
            "shop_id": self.shop_id,
            "partner_id": self.partner_id,
            "refresh_token": self.refresh_token,
        }

        try:
            response = requests.post(
                url,
                json=body,
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
            data = response.json()

            if "access_token" in data and data["access_token"]:
                new_access_token = data["access_token"]
                new_refresh_token = data.get(
                    "refresh_token", self.refresh_token
                )
                expire_in = data.get("expire_in", 14400)  # Default 4 hours

                # Update tokens
                self.access_token = new_access_token
                self.refresh_token = new_refresh_token
                self.token_expiry = pendulum.now().add(seconds=expire_in)

                # Persist to pipeline state
                save_tokens_to_state(
                    platform=PLATFORM_NAME,
                    access_token=new_access_token,
                    refresh_token=new_refresh_token,
                    token_expiry=self.token_expiry,
                )

                logger.info(
                    f"Shopee token refreshed successfully. "
                    f"Expires in {expire_in} seconds."
                )
                return True
            else:
                error = data.get("error", "Unknown error")
                message = data.get("message", "")
                logger.error(
                    f"Failed to refresh Shopee token: {error} - {message}"
                )
                return False

        except Exception as e:
            logger.error(f"Exception during Shopee token refresh: {e}")
            return False

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        """Add Shopee authentication parameters to the request."""
        # Check pipeline state for stored tokens (first call only)
        self._check_state_for_tokens()

        # Check if token needs refresh
        if self.auto_refresh and self._is_token_expired():
            if not self._refresh_access_token():
                logger.warning(
                    "Token refresh failed, continuing with current token"
                )

        # Extract API path from URL
        if request.url:
            parsed = urlparse(request.url)
            path = parsed.path
        else:
            path = "/"

        # Generate timestamp and signature
        timestamp = self._get_timestamp()
        signature = self._generate_signature(path, timestamp)

        # Prepare authentication parameters
        auth_params = {
            "partner_id": str(self.partner_id),
            "shop_id": str(self.shop_id),
            "access_token": self.access_token,
            "timestamp": str(timestamp),
            "sign": signature,
        }

        # Get existing query parameters
        parsed = urlparse(request.url)
        existing_params = parse_qs(parsed.query)

        # Flatten lists in existing_params (parse_qs returns lists)
        flattened_params = {
            k: v[0] if isinstance(v, list) and len(v) == 1 else v
            for k, v in existing_params.items()
        }

        # Merge all parameters
        all_params = {**flattened_params, **auth_params}

        # Rebuild URL with authentication parameters
        new_query = urlencode(all_params, doseq=True)
        request.url = urlunparse(
            (
                parsed.scheme,
                parsed.netloc,
                parsed.path,
                parsed.params,
                new_query,
                parsed.fragment,
            )
        )

        return request
