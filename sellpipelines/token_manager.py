"""Token Manager for OAuth tokens using dlt Pipeline State.

This module provides utilities for storing and retrieving OAuth tokens
from dlt's pipeline state, enabling automatic token refresh and persistence
across pipeline runs.
"""

from typing import Optional, TypedDict

import dlt
from dlt.common import logger
from dlt.common.pendulum import pendulum


class TokenData(TypedDict):
    """Structure for storing OAuth token data."""

    access_token: str
    refresh_token: str
    token_expiry: str  # ISO format timestamp


def get_token_state_key(platform: str) -> str:
    """Get the state key for a platform's tokens."""
    return f"{platform}_oauth_tokens"


def load_tokens_from_state(platform: str) -> Optional[TokenData]:
    """Load OAuth tokens from dlt pipeline state.

    Args:
        platform: Platform name (e.g., 'shopee', 'lazada', 'redmart')

    Returns:
        TokenData if found, None otherwise
    """
    try:
        state = dlt.current.source_state()
        key = get_token_state_key(platform)
        tokens = state.get(key)

        if tokens:
            logger.info(f"Loaded {platform} tokens from pipeline state")
            return tokens
        return None
    except Exception as e:
        logger.debug(f"Could not load tokens from state: {e}")
        return None


def save_tokens_to_state(
    platform: str,
    access_token: str,
    refresh_token: str,
    token_expiry: pendulum.DateTime,
) -> None:
    """Save OAuth tokens to dlt pipeline state.

    Args:
        platform: Platform name (e.g., 'shopee', 'lazada', 'redmart')
        access_token: The new access token
        refresh_token: The new refresh token
        token_expiry: When the access token expires
    """
    try:
        state = dlt.current.source_state()
        key = get_token_state_key(platform)

        state[key] = TokenData(
            access_token=access_token,
            refresh_token=refresh_token,
            token_expiry=token_expiry.isoformat(),
        )

        logger.info(
            f"Saved {platform} tokens to pipeline state. "
            f"Expires: {token_expiry.isoformat()}"
        )
    except Exception as e:
        logger.warning(f"Could not save tokens to state: {e}")


def is_token_expired(
    token_expiry: pendulum.DateTime, buffer_minutes: int = 5
) -> bool:
    """Check if a token is expired or about to expire.

    Args:
        token_expiry: When the token expires
        buffer_minutes: Minutes before expiry to consider it expired

    Returns:
        True if token is expired or will expire within buffer time
    """
    return pendulum.now() >= token_expiry.subtract(minutes=buffer_minutes)


def parse_token_expiry(
    expiry_str: Optional[str],
) -> Optional[pendulum.DateTime]:
    """Parse token expiry from ISO format string.

    Args:
        expiry_str: ISO format timestamp string

    Returns:
        pendulum.DateTime or None if parsing fails
    """
    if not expiry_str:
        return None
    try:
        return pendulum.parse(expiry_str)
    except Exception:
        return None
