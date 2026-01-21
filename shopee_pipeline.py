"""Shopee API Pipeline using DLT REST API Source.

This pipeline fetches product data from Shopee Open Platform using:
- Custom HMAC-SHA256 signature-based authentication
- Offset-based pagination with custom paginator
- Two-step process: get_item_list -> get_item_base_info
- duckdb as destination
"""

import os
from typing import Any, Iterator, List, Optional

import dlt
import requests
from dlt.sources.helpers.rest_client import paginate
from dlt.sources.helpers.rest_client.paginators import BasePaginator
from dotenv import load_dotenv
from requests import Request, Response

from shopeeclient.auth import ShopeeAuth

# Load environment variables
load_dotenv()

# Required credentials
PARTNER_ID = os.getenv("SHOPEE_APP_ID")
PARTNER_KEY = os.getenv("SHOPEE_APP_KEY")
SHOP_ID = os.getenv("SHOPEE_SHOP_ID")
ACCESS_TOKEN = os.getenv("SHOPEE_ACCESS_TOKEN")
REFRESH_TOKEN = os.getenv("SHOPEE_REFRESH_TOKEN")

if not PARTNER_ID or not PARTNER_KEY:
    raise ValueError("Missing SHOPEE_APP_ID or SHOPEE_APP_KEY in .env file")

if not SHOP_ID:
    raise ValueError("Missing SHOPEE_SHOP_ID in .env file")

if not ACCESS_TOKEN:
    raise ValueError(
        "Missing SHOPEE_ACCESS_TOKEN in .env file. "
        "Use shopee_auth.py to obtain an access token first."
    )

# Note: REFRESH_TOKEN is optional but recommended for auto-refresh
if not REFRESH_TOKEN:
    print(
        "Warning: SHOPEE_REFRESH_TOKEN not set. "
        "Auto-refresh will not work when token expires."
    )


def extract_shopee_fields(product: dict) -> dict:
    """Extract only the required fields from a Shopee product."""
    # Get image from the image object
    image_data = product.get("image", {})
    image_url_list = image_data.get("image_url_list", [])
    first_image = image_url_list[0] if image_url_list else None

    # Get stock info from stock_info_v2
    stock_info = product.get("stock_info_v2", {})
    seller_stock = stock_info.get("seller_stock", [])
    stock = (
        seller_stock[0].get("stock")
        if seller_stock
        else stock_info.get("total_available_stock")
    )

    # Get barcode - try gtin_code first, fallback to item_sku
    barcode = product.get("gtin_code")
    if not barcode or barcode == "00":  # "00" means no gtin_code
        barcode = product.get("item_sku")

    return {
        "platform_id": str(product.get("item_id")),
        "product_name": product.get("item_name"),
        "barcode": barcode,
        "image_url": first_image,
        "stock": stock,
        "store_id": "shopee",
    }


class ShopeePaginator(BasePaginator):
    """Custom paginator for Shopee API using next_offset."""

    def __init__(self):
        super().__init__()
        self.offset = 0

    def update_state(
        self, response: Response, data: Optional[List[Any]] = None
    ) -> None:
        """Update pagination state from response."""
        response_data = response.json()

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
def get_item_ids(
    partner_id: int = dlt.secrets.value,
    partner_key: str = dlt.secrets.value,
    shop_id: int = dlt.secrets.value,
    access_token: str = dlt.secrets.value,
    refresh_token: Optional[str] = None,
) -> Iterator[int]:
    """
    Fetch all item IDs from Shopee API using get_item_list endpoint.

    Args:
        partner_id: Shopee partner/app ID
        partner_key: Shopee partner/app key
        shop_id: Shop ID
        access_token: OAuth access token
        refresh_token: OAuth refresh token for auto-refresh

    Yields:
        Item IDs
    """
    # Base URL and endpoint
    base_url = "https://partner.shopeemobile.com"
    endpoint = "/api/v2/product/get_item_list"

    # Create authentication with auto-refresh support
    auth = ShopeeAuth(
        partner_id=partner_id,
        partner_key=partner_key,
        shop_id=shop_id,
        access_token=access_token,
        refresh_token=refresh_token,
    )

    # Configure paginator
    paginator = ShopeePaginator()

    # Paginate through all items
    for page in paginate(
        url=f"{base_url}{endpoint}",
        auth=auth,
        paginator=paginator,
        data_selector="response.item",  # Extract items directly from response
        params={
            "offset": 0,
            "page_size": 50,  # Max page size
            "item_status": "NORMAL",  # Only get active items
        },
    ):
        # page is now a PageData (list) containing the items directly
        for item in page:
            if isinstance(item, dict):
                yield item.get("item_id")


@dlt.resource(
    name="products",
    write_disposition="merge",
    primary_key=["platform_id", "store_id"],
)
def get_products(
    partner_id: int = dlt.secrets.value,
    partner_key: str = dlt.secrets.value,
    shop_id: int = dlt.secrets.value,
    access_token: str = dlt.secrets.value,
    refresh_token: Optional[str] = None,
) -> Iterator[Any]:
    """
    Fetch detailed product information from Shopee API.

    This resource depends on item_ids resource to get the list of items,
    then fetches detailed info in batches using get_item_base_info endpoint.

    Args:
        partner_id: Shopee partner/app ID
        partner_key: Shopee partner/app key
        shop_id: Shop ID
        access_token: OAuth access token
        refresh_token: OAuth refresh token for auto-refresh

    Yields:
        Product records with: platform_id, product_name, barcode, image_url, stock, store_id
    """
    # Get all item IDs first
    item_ids: List[int] = []
    for item_id in get_item_ids(
        partner_id, partner_key, shop_id, access_token, refresh_token
    ):
        if item_id is not None:
            item_ids.append(item_id)

    if not item_ids:
        print("No items found")
        return

    print(f"Found {len(item_ids)} items, fetching details...")

    # Base URL and endpoint for detailed info
    base_url = "https://partner.shopeemobile.com"
    endpoint = "/api/v2/product/get_item_base_info"

    # Create authentication with auto-refresh support
    auth = ShopeeAuth(
        partner_id=partner_id,
        partner_key=partner_key,
        shop_id=shop_id,
        access_token=access_token,
        refresh_token=refresh_token,
    )

    # Fetch item details in batches of 50 (API limit)
    batch_size = 50
    session = requests.Session()  # Reuse session for efficiency

    for i in range(0, len(item_ids), batch_size):
        batch = item_ids[i : i + batch_size]

        # Build the request using requests library (not httpx)
        url = f"{base_url}{endpoint}"
        params = {
            "item_id_list": ",".join(str(id) for id in batch),
            "need_tax_info": "false",
            "need_complaint_policy": "false",
        }

        # Create a requests.Request and prepare it
        req = Request("GET", url, params=params)
        prepared = req.prepare()

        # Apply Shopee authentication (adds signature, timestamp, etc.)
        prepared = auth(prepared)

        # Make the request
        response = session.send(prepared)
        data = response.json()

        # Extract products from response and filter to required fields
        if "response" in data and "item_list" in data["response"]:
            products = data["response"]["item_list"]
            for product in products:
                yield extract_shopee_fields(product)
        elif "error" in data and data.get("error"):
            print(f"API Error: {data.get('error')} - {data.get('message')}")


@dlt.source
def shopee_source(
    partner_id: int = dlt.secrets.value,
    partner_key: str = dlt.secrets.value,
    shop_id: int = dlt.secrets.value,
    access_token: str = dlt.secrets.value,
    refresh_token: Optional[str] = None,
):
    """
    DLT source for Shopee Open Platform.

    Args:
        partner_id: Shopee partner/app ID
        partner_key: Shopee partner/app key
        shop_id: Shop ID
        access_token: OAuth access token
        refresh_token: OAuth refresh token for auto-refresh

    Returns:
        DLT source with products resource
    """
    return get_products(
        partner_id=partner_id,
        partner_key=partner_key,
        shop_id=shop_id,
        access_token=access_token,
        refresh_token=refresh_token,
    )


def run_pipeline(
    destination: str = "duckdb",
    dataset_name: str = "sell_data",
    dev_mode: bool = False,
) -> None:
    """
    Run the Shopee pipeline.

    Args:
        destination: DLT destination
        dataset_name: Name of the dataset
        dev_mode: Enable dev mode for development (resets schema/state)
    """
    # Create pipeline - use data.duckdb as the shared database
    pipeline = dlt.pipeline(
        pipeline_name="shopee_pipeline",
        destination=dlt.destinations.duckdb("data.duckdb"),
        dataset_name=dataset_name,
        dev_mode=dev_mode,
        progress="log",  # Show progress in logs
    )

    # Load data
    load_info = pipeline.run(
        shopee_source(
            partner_id=int(PARTNER_ID),
            partner_key=PARTNER_KEY,
            shop_id=int(SHOP_ID),
            access_token=ACCESS_TOKEN,
            refresh_token=REFRESH_TOKEN,
        )
    )

    # Print load information
    print("\n" + "=" * 50)
    print("Pipeline completed successfully!")
    print("=" * 50)
    print(f"\nLoaded {len(load_info.loads_ids)} load(s)")
    print(
        f"Load ID: {load_info.loads_ids[0] if load_info.loads_ids else 'N/A'}"
    )

    # Print table statistics
    if hasattr(load_info, "load_packages") and load_info.load_packages:
        for package in load_info.load_packages:
            if hasattr(package, "schema_update"):
                print(f"\nSchema: {package.schema.name}")
                for table_name in package.schema.tables:
                    print(f"  - {table_name}")


if __name__ == "__main__":
    # Run with duckdb - data.duckdb, sell_data schema, products table
    run_pipeline()
