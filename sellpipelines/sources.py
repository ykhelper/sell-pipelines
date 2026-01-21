"""DLT sources for e-commerce platforms.

This module defines dlt sources for Shopee, Redmart, and Lazada
with proper error handling and unified data schema for Dagster integration.
"""

from typing import Any, Iterator, List, Optional

import dlt
import requests
from dlt.common import logger
from dlt.sources.helpers.rest_client import paginate
from dlt.sources.helpers.rest_client.paginators import (
    BasePaginator,
    OffsetPaginator,
    PageNumberPaginator,
)
from requests import Request, Response

from lazadaclient.auth import LazadaAuth
from shopeeclient.auth import ShopeeAuth

from .transformers import (
    extract_lazada_fields,
    extract_redmart_fields,
    extract_shopee_fields,
)

# =============================================================================
# SHOPEE SOURCE
# =============================================================================


class ShopeePaginator(BasePaginator):
    """Custom paginator for Shopee API using next_offset with error handling."""

    def __init__(self):
        super().__init__()
        self.offset = 0
        self._error: Optional[str] = None

    def update_state(
        self, response: Response, data: Optional[List[Any]] = None
    ) -> None:
        """Update pagination state from response with error handling."""
        try:
            response_data = response.json()
        except Exception as e:
            logger.error(f"Failed to parse Shopee response: {e}")
            self._has_next_page = False
            self._error = str(e)
            return

        # Check for API errors
        if "error" in response_data and response_data.get("error"):
            error_msg = response_data.get("message", "Unknown error")
            logger.error(
                f"Shopee API error: {response_data.get('error')} - {error_msg}"
            )
            self._has_next_page = False
            self._error = error_msg
            return

        if "response" in response_data:
            resp = response_data["response"]
            has_next = resp.get("has_next_page", False)

            if has_next:
                self.offset = resp.get("next_offset", self.offset + 50)
                self._has_next_page = True
            else:
                self._has_next_page = False
        else:
            self._has_next_page = False

    def update_request(self, request: Request) -> None:
        """Update request with current offset."""
        if request.params is None:
            request.params = {}
        request.params["offset"] = self.offset


@dlt.resource(
    name="item_ids",
    write_disposition="replace",
)
def get_shopee_item_ids(
    partner_id: int,
    partner_key: str,
    shop_id: int,
    access_token: str,
    refresh_token: Optional[str] = None,
) -> Iterator[int]:
    """Fetch all item IDs from Shopee API with error handling.

    Args:
        partner_id: Shopee partner/app ID
        partner_key: Shopee partner/app key
        shop_id: Shop ID
        access_token: OAuth access token
        refresh_token: OAuth refresh token for auto-refresh

    Yields:
        Item IDs
    """
    base_url = "https://partner.shopeemobile.com"
    endpoint = "/api/v2/product/get_item_list"

    auth = ShopeeAuth(
        partner_id=partner_id,
        partner_key=partner_key,
        shop_id=shop_id,
        access_token=access_token,
        refresh_token=refresh_token,
    )

    paginator = ShopeePaginator()

    for page in paginate(
        url=f"{base_url}{endpoint}",
        auth=auth,
        paginator=paginator,
        params={
            "offset": 0,
            "page_size": 50,
            "item_status": "NORMAL",
        },
    ):
        # Handle error responses
        if isinstance(page, dict):
            if "error" in page and page.get("error"):
                logger.error(
                    f"Shopee API error: {page.get('error')} - {page.get('message')}"
                )
                continue

            if "response" in page:
                items = page["response"].get("item", [])
                for item in items:
                    item_id = item.get("item_id")
                    if item_id is not None:
                        yield item_id


@dlt.resource(
    name="products",
    write_disposition="merge",
    primary_key=["platform_id", "store_id"],
)
def get_shopee_products(
    partner_id: int,
    partner_key: str,
    shop_id: int,
    access_token: str,
    refresh_token: Optional[str] = None,
) -> Iterator[Any]:
    """Fetch detailed product information from Shopee API.

    Args:
        partner_id: Shopee partner/app ID
        partner_key: Shopee partner/app key
        shop_id: Shop ID
        access_token: OAuth access token
        refresh_token: OAuth refresh token for auto-refresh

    Yields:
        Normalized product records with unified schema
    """
    # Collect all item IDs first
    item_ids: List[int] = list(
        get_shopee_item_ids(
            partner_id, partner_key, shop_id, access_token, refresh_token
        )
    )

    if not item_ids:
        logger.warning("No Shopee items found")
        return

    logger.info(f"Found {len(item_ids)} Shopee items, fetching details...")

    base_url = "https://partner.shopeemobile.com"
    endpoint = "/api/v2/product/get_item_base_info"

    auth = ShopeeAuth(
        partner_id=partner_id,
        partner_key=partner_key,
        shop_id=shop_id,
        access_token=access_token,
        refresh_token=refresh_token,
    )

    # Fetch item details in batches of 50 (API limit)
    batch_size = 50
    session = requests.Session()

    for i in range(0, len(item_ids), batch_size):
        batch = item_ids[i : i + batch_size]

        # Build the request using requests library
        url = f"{base_url}{endpoint}"
        params = {
            "item_id_list": ",".join(str(id) for id in batch),
            "need_tax_info": "false",
            "need_complaint_policy": "false",
        }

        # Create and prepare request
        req = Request("GET", url, params=params)
        prepared = req.prepare()

        # Apply Shopee authentication (adds signature, timestamp, etc.)
        prepared = auth(prepared)

        try:
            # Make the request
            response = session.send(prepared, timeout=30)
            data = response.json()

            # Check for API errors
            if "error" in data and data.get("error"):
                logger.error(
                    f"Shopee API error: {data.get('error')} - {data.get('message')}"
                )
                continue

            # Extract and transform products
            if "response" in data and "item_list" in data["response"]:
                products = data["response"]["item_list"]
                for product in products:
                    yield extract_shopee_fields(product)

        except requests.RequestException as e:
            logger.error(f"Request failed for Shopee batch {i}: {e}")
            continue
        except Exception as e:
            logger.error(f"Error processing Shopee batch {i}: {e}")
            continue


@dlt.source
def shopee_source(
    partner_id: int,
    partner_key: str,
    shop_id: int,
    access_token: str,
    refresh_token: Optional[str] = None,
):
    """DLT source for Shopee Open Platform.

    Args:
        partner_id: Shopee partner/app ID
        partner_key: Shopee partner/app key
        shop_id: Shop ID
        access_token: OAuth access token
        refresh_token: OAuth refresh token for auto-refresh

    Returns:
        DLT source with products resource
    """
    return get_shopee_products(
        partner_id=partner_id,
        partner_key=partner_key,
        shop_id=shop_id,
        access_token=access_token,
        refresh_token=refresh_token,
    )


# =============================================================================
# REDMART SOURCE
# =============================================================================


class RedmartPaginator(PageNumberPaginator):
    """Page number paginator for Redmart with error handling."""

    def __init__(self):
        super().__init__(
            base_page=1,
            page=1,
            page_param="page",
            total_path="result.total",
            maximum_page=None,
        )
        self._error: Optional[str] = None

    def update_state(
        self, response: Response, data: Optional[List[Any]] = None
    ) -> None:
        """Update pagination state with error handling."""
        try:
            response_data = response.json()
        except Exception as e:
            logger.error(f"Failed to parse Redmart response: {e}")
            self._has_next_page = False
            self._error = str(e)
            return

        # Check for API errors
        if "code" in response_data and response_data.get("code") != "0":
            error_msg = response_data.get("message", "Unknown error")
            logger.error(
                f"Redmart API error: {response_data.get('code')} - {error_msg}"
            )
            self._has_next_page = False
            self._error = error_msg
            return

        # Call parent implementation
        super().update_state(response, data)


@dlt.resource(
    name="products",
    write_disposition="merge",
    primary_key=["platform_id", "store_id"],
)
def get_redmart_products(
    app_key: str,
    app_secret: str,
    access_token: str,
    store_id: str,
    refresh_token: Optional[str] = None,
) -> Iterator[Any]:
    """Fetch products from Redmart API with page-based pagination.

    Args:
        app_key: Redmart application key
        app_secret: Redmart application secret
        access_token: OAuth access token for seller APIs
        store_id: Redmart store ID
        refresh_token: OAuth refresh token for auto-refresh

    Yields:
        Normalized product records with unified schema
    """
    base_url = "https://api.lazada.sg/rest"
    endpoint = "/rss/products/get"

    auth = LazadaAuth(
        app_key=app_key,
        app_secret=app_secret,
        access_token=access_token,
        refresh_token=refresh_token,
        platform="redmart",
    )

    paginator = RedmartPaginator()

    for page in paginate(
        url=f"{base_url}{endpoint}",
        auth=auth,
        paginator=paginator,
        params={
            "storeId": store_id,
            "pageSize": 100,
            "page": 1,
        },
    ):
        # Handle different response formats
        if isinstance(page, dict):
            # Check for API errors
            if "code" in page and page.get("code") != "0":
                logger.error(
                    f"Redmart API error: {page.get('code')} - {page.get('message')}"
                )
                continue

            if "result" in page and "data" in page["result"]:
                products = page["result"]["data"]
                if isinstance(products, list):
                    for product in products:
                        yield extract_redmart_fields(product)
                else:
                    yield extract_redmart_fields(products)
            else:
                # Unexpected format
                logger.warning(
                    f"Unexpected Redmart response format: {list(page.keys())}"
                )
        else:
            logger.warning(f"Unexpected Redmart page type: {type(page)}")


@dlt.source
def redmart_source(
    app_key: str,
    app_secret: str,
    access_token: str,
    store_id: str,
    refresh_token: Optional[str] = None,
):
    """DLT source for Redmart API (via Lazada Open Platform).

    Args:
        app_key: Redmart application key
        app_secret: Redmart application secret
        access_token: OAuth access token for seller APIs
        store_id: Redmart store ID
        refresh_token: OAuth refresh token for auto-refresh

    Returns:
        DLT source with products resource
    """
    return get_redmart_products(
        app_key=app_key,
        app_secret=app_secret,
        access_token=access_token,
        store_id=store_id,
        refresh_token=refresh_token,
    )


# =============================================================================
# LAZADA SOURCE
# =============================================================================


class LazadaPaginator(OffsetPaginator):
    """Offset paginator for Lazada with error handling."""

    def __init__(self):
        super().__init__(
            limit=100,
            offset=0,
            offset_param="offset",
            limit_param="limit",
            total_path="data.total_products",
        )
        self._error: Optional[str] = None

    def update_state(
        self, response: Response, data: Optional[List[Any]] = None
    ) -> None:
        """Update pagination state with error handling."""
        try:
            response_data = response.json()
        except Exception as e:
            logger.error(f"Failed to parse Lazada response: {e}")
            self._has_next_page = False
            self._error = str(e)
            return

        # Check for API errors
        if "code" in response_data and response_data.get("code") != "0":
            error_msg = response_data.get("message", "Unknown error")
            logger.error(
                f"Lazada API error: {response_data.get('code')} - {error_msg}"
            )
            self._has_next_page = False
            self._error = error_msg
            return

        # Call parent implementation
        super().update_state(response, data)


@dlt.resource(
    name="products",
    write_disposition="merge",
    primary_key=["platform_id", "store_id"],
)
def get_lazada_products(
    app_key: str,
    app_secret: str,
    access_token: str,
    refresh_token: Optional[str] = None,
) -> Iterator[Any]:
    """Fetch products from Lazada API with pagination.

    Args:
        app_key: Lazada application key
        app_secret: Lazada application secret
        access_token: OAuth access token for seller APIs
        refresh_token: OAuth refresh token for auto-refresh

    Yields:
        Normalized product records with unified schema
    """
    base_url = "https://api.lazada.sg/rest"
    endpoint = "/products/get"

    auth = LazadaAuth(
        app_key=app_key,
        app_secret=app_secret,
        access_token=access_token,
        refresh_token=refresh_token,
        platform="lazada",
    )

    paginator = LazadaPaginator()

    for page in paginate(
        url=f"{base_url}{endpoint}",
        auth=auth,
        paginator=paginator,
        params={
            "limit": 100,
            "offset": 0,
        },
    ):
        # Handle different response formats
        if isinstance(page, dict):
            # Check for API errors
            if "code" in page and page.get("code") != "0":
                logger.error(
                    f"Lazada API error: {page.get('code')} - {page.get('message')}"
                )
                continue

            if "data" in page and "products" in page["data"]:
                products = page["data"]["products"]
                if isinstance(products, list):
                    for product in products:
                        yield extract_lazada_fields(product)
                else:
                    yield extract_lazada_fields(products)
            else:
                # Unexpected format
                logger.warning(
                    f"Unexpected Lazada response format: {list(page.keys())}"
                )
        else:
            logger.warning(f"Unexpected Lazada page type: {type(page)}")


@dlt.source
def lazada_source(
    app_key: str,
    app_secret: str,
    access_token: str,
    refresh_token: Optional[str] = None,
):
    """DLT source for Lazada Seller API (Singapore).

    Args:
        app_key: Lazada application key
        app_secret: Lazada application secret
        access_token: OAuth access token for seller APIs
        refresh_token: OAuth refresh token for auto-refresh

    Returns:
        DLT source with products resource
    """
    return get_lazada_products(
        app_key=app_key,
        app_secret=app_secret,
        access_token=access_token,
        refresh_token=refresh_token,
    )
