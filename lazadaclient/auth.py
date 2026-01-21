"""Custom DLT Authentication for Lazada/Redmart API with signature and auto token refresh."""

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

from .signature import generate_signature

# Auth endpoint for token refresh
AUTH_URL = "https://auth.lazada.com/rest"


@configspec
class LazadaAuth(AuthConfigBase):
    """Custom authentication for Lazada/Redmart API with signature and auto token refresh.

    Lazada requires:
    - app_key: Application key
    - app_secret: Application secret
    - sign_method: Always "sha256"
    - timestamp: Current time in milliseconds
    - sign: HMAC-SHA256 signature of api_path + sorted parameters
    - access_token: OAuth access token (optional, required for seller APIs)
    - refresh_token: OAuth refresh token (for auto-refresh)

    Token refresh is automatic when the access token expires.
    Tokens are persisted in dlt pipeline state.
    """

    def __init__(
        self,
        app_key: str,
        app_secret: str,
        access_token: Optional[str] = None,
        refresh_token: Optional[str] = None,
        token_expiry_seconds: Optional[int] = None,
        platform: str = "lazada",  # "lazada" or "redmart"
        auto_refresh: bool = True,
    ):
        super().__init__()
        self.app_key = app_key
        self.app_secret = app_secret
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.platform = platform
        self.auto_refresh = auto_refresh
        self._state_checked = False

        # Token expiry tracking
        # Lazada tokens typically expire in 7 days, but we default to 6 hours for safety
        if token_expiry_seconds:
            self.token_expiry = pendulum.now().add(seconds=token_expiry_seconds)
        else:
            # Default: assume token is valid for 6 hours (to be safe)
            self.token_expiry = pendulum.now().add(hours=6)

    def _check_state_for_tokens(self) -> None:
        """Check pipeline state for stored tokens (only once per session)."""
        if self._state_checked:
            return

        self._state_checked = True

        try:
            stored_tokens = load_tokens_from_state(self.platform)
            if stored_tokens:
                stored_expiry = parse_token_expiry(
                    stored_tokens.get("token_expiry")
                )

                # Use stored tokens if they're newer/valid
                if stored_expiry and not is_token_expired(stored_expiry):
                    self.access_token = stored_tokens["access_token"]
                    self.refresh_token = stored_tokens["refresh_token"]
                    self.token_expiry = stored_expiry
                    logger.info(
                        f"Using {self.platform} tokens from pipeline state"
                    )
                elif stored_tokens.get("refresh_token"):
                    # Tokens expired but we have refresh token from state
                    self.refresh_token = stored_tokens["refresh_token"]
                    logger.info(
                        f"{self.platform} tokens expired, will refresh using stored refresh token"
                    )
        except Exception as e:
            logger.debug(f"Could not check state for tokens: {e}")

    def _get_timestamp(self) -> str:
        """Get current timestamp in milliseconds."""
        return str(int(time.time() * 1000))

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
                f"No refresh token available for {self.platform}, cannot refresh access token"
            )
            return False

        logger.info(f"Refreshing {self.platform} access token...")

        api_path = "/auth/token/refresh"

        params = {
            "app_key": self.app_key,
            "sign_method": "sha256",
            "timestamp": self._get_timestamp(),
            "refresh_token": self.refresh_token,
        }

        signature = generate_signature(self.app_secret, api_path, params)
        params["sign"] = signature

        url = f"{AUTH_URL}{api_path}"

        try:
            response = requests.post(url, data=params, timeout=30)
            data = response.json()

            if "access_token" in data and data["access_token"]:
                new_access_token = data["access_token"]
                new_refresh_token = data.get(
                    "refresh_token", self.refresh_token
                )
                # Lazada returns expires_in in seconds
                expire_in = data.get("expires_in", 604800)  # Default 7 days

                # Update tokens
                self.access_token = new_access_token
                self.refresh_token = new_refresh_token
                self.token_expiry = pendulum.now().add(seconds=expire_in)

                # Persist to pipeline state
                save_tokens_to_state(
                    platform=self.platform,
                    access_token=new_access_token,
                    refresh_token=new_refresh_token,
                    token_expiry=self.token_expiry,
                )

                logger.info(
                    f"{self.platform} token refreshed successfully. "
                    f"Expires in {expire_in} seconds."
                )
                return True
            else:
                error_code = data.get("code", "Unknown")
                error_msg = data.get("message", "Unknown error")
                logger.error(
                    f"Failed to refresh {self.platform} token: {error_code} - {error_msg}"
                )
                return False

        except Exception as e:
            logger.error(f"Exception during {self.platform} token refresh: {e}")
            return False

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        """Add Lazada authentication parameters to the request."""
        # Check pipeline state for stored tokens (first call only)
        self._check_state_for_tokens()

        # Check if token needs refresh
        if self.auto_refresh and self.access_token and self._is_token_expired():
            if not self._refresh_access_token():
                logger.warning(
                    "Token refresh failed, continuing with current token"
                )

        # Get the API path from the URL
        # URL format: https://api.lazada.sg/rest/products/get
        # We need: /products/get
        if request.url:
            # Extract path after /rest
            parts = request.url.split("/rest", 1)
            api_path = parts[1].split("?")[0] if len(parts) > 1 else "/"
        else:
            api_path = "/"

        # Prepare system parameters
        system_params = {
            "app_key": self.app_key,
            "sign_method": "sha256",
            "timestamp": self._get_timestamp(),
        }

        if self.access_token:
            system_params["access_token"] = self.access_token

        # Get existing query parameters
        existing_params = {}
        if request.url and "?" in request.url:
            parsed = urlparse(request.url)
            # parse_qs returns lists, we need single values
            existing_params = {
                k: v[0] if isinstance(v, list) and len(v) == 1 else v
                for k, v in parse_qs(parsed.query).items()
            }

        # Merge all parameters for signature generation
        all_params = {**existing_params, **system_params}

        # Generate signature
        signature = generate_signature(self.app_secret, api_path, all_params)
        all_params["sign"] = signature

        # Update request with new parameters
        # For GET requests, update URL params
        if request.method == "GET":
            parsed = urlparse(request.url)
            # Rebuild URL with all parameters
            new_query = urlencode(all_params)
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
        # For POST requests, add to body
        else:
            request.prepare_body(data=all_params, files=None)

        return request
