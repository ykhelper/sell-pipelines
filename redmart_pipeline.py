"""Redmart API Pipeline using DLT REST API Source.

This pipeline fetches product data from Redmart API (via Lazada Open Platform) using:
- Custom signature-based authentication (same as Lazada)
- Page-based pagination (starting at page 1)
- duckdb as destination
"""

import os
from typing import Any, Iterator, Optional

import dlt
from dlt.sources.helpers.rest_client import paginate
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator
from dotenv import load_dotenv

from lazadaclient.auth import LazadaAuth

# Load environment variables
load_dotenv()

# Required credentials
APP_KEY = os.getenv("REDMART_APP_KEY")
APP_SECRET = os.getenv("REDMART_APP_SECRET")
ACCESS_TOKEN = os.getenv("REDMART_ACCESS_TOKEN")
REFRESH_TOKEN = os.getenv("REDMART_REFRESH_TOKEN")
STORE_ID = os.getenv("REDMART_STORE_ID")

if not APP_KEY or not APP_SECRET:
    raise ValueError(
        "Missing REDMART_APP_KEY or REDMART_APP_SECRET in .env file"
    )

if not ACCESS_TOKEN:
    raise ValueError(
        "Missing REDMART_ACCESS_TOKEN in .env file. "
        "Use test.py to obtain an access token first."
    )

# Note: REFRESH_TOKEN is optional but recommended for auto-refresh
if not REFRESH_TOKEN:
    print(
        "Warning: REDMART_REFRESH_TOKEN not set. "
        "Auto-refresh will not work when token expires."
    )

if not STORE_ID:
    raise ValueError("Missing REDMART_STORE_ID in .env file")


def extract_redmart_fields(product: dict) -> dict:
    """Extract only the required fields from a Redmart product.

    Note: The Redmart API (/rss/products/get) does not return image_url or stock
    in its response. These fields will be None.
    """
    # Get first barcode from barcodes array
    barcodes = product.get("barcodes", [])
    first_barcode = barcodes[0] if barcodes else None

    return {
        "platform_id": str(product.get("rpc")),
        "product_name": product.get("title"),
        "barcode": first_barcode,
        "image_url": None,  # Not available in Redmart API response
        "stock": None,  # Not available in Redmart API response
        "store_id": "redmart",
    }


@dlt.resource(
    name="products",
    write_disposition="merge",
    primary_key=["platform_id", "store_id"],
)
def get_products(
    app_key: str = dlt.secrets.value,
    app_secret: str = dlt.secrets.value,
    access_token: str = dlt.secrets.value,
    refresh_token: Optional[str] = None,
    store_id: str = dlt.secrets.value,
) -> Iterator[Any]:
    """
    Fetch products from Redmart API with page-based pagination.

    Args:
        app_key: Redmart application key
        app_secret: Redmart application secret
        access_token: OAuth access token for seller APIs
        refresh_token: OAuth refresh token for auto-refresh
        store_id: Redmart store ID

    Yields:
        Product records with: platform_id, product_name, barcode, image_url, stock, store_id
        Note: image_url and stock are not available in Redmart API
    """
    # Base URL and endpoint
    base_url = "https://api.lazada.sg/rest"
    endpoint = "/rss/products/get"

    # Create authentication with auto-refresh support
    auth = LazadaAuth(
        app_key=app_key,
        app_secret=app_secret,
        access_token=access_token,
        refresh_token=refresh_token,
        platform="redmart",
    )

    # Configure page-based paginator
    # Redmart uses page numbers starting at 1 (not 0)
    paginator = PageNumberPaginator(
        base_page=1,  # First page is 1, not 0
        page=1,  # Start at page 1
        page_param="page",
        total_path="result.total",  # Path to total count in response
        maximum_page=None,  # No hard limit, paginate until empty
    )

    # Paginate through all products
    for page in paginate(
        url=f"{base_url}{endpoint}",
        auth=auth,
        paginator=paginator,
        params={
            "storeId": store_id,
            "pageSize": 100,  # Max page size
            "page": 1,  # Starting page
        },
    ):
        # Extract products from response
        # Response structure: {"result": {"data": [...], "total": "15"}, "code": "0"}
        if (
            isinstance(page, dict)
            and "result" in page
            and "data" in page["result"]
        ):
            products = page["result"]["data"]
            if isinstance(products, list):
                for product in products:
                    yield extract_redmart_fields(product)
            else:
                yield extract_redmart_fields(products)
        else:
            # If page is already a product dict
            yield extract_redmart_fields(page)


@dlt.source
def redmart_source(
    app_key: str = dlt.secrets.value,
    app_secret: str = dlt.secrets.value,
    access_token: str = dlt.secrets.value,
    refresh_token: Optional[str] = None,
    store_id: str = dlt.secrets.value,
):
    """
    DLT source for Redmart API (via Lazada Open Platform).

    Args:
        app_key: Redmart application key
        app_secret: Redmart application secret
        access_token: OAuth access token for seller APIs
        refresh_token: OAuth refresh token for auto-refresh
        store_id: Redmart store ID

    Returns:
        DLT source with products resource
    """
    return get_products(
        app_key=app_key,
        app_secret=app_secret,
        access_token=access_token,
        refresh_token=refresh_token,
        store_id=store_id,
    )


def run_pipeline(
    destination: str = "duckdb",
    dataset_name: str = "sell_data",
    dev_mode: bool = False,
) -> None:
    # Create pipeline - use data.duckdb as the shared database
    pipeline = dlt.pipeline(
        pipeline_name="redmart_pipeline",
        destination=dlt.destinations.duckdb("data.duckdb"),
        dataset_name=dataset_name,
        dev_mode=dev_mode,
        progress="log",  # Show progress in logs
    )

    # Load data
    load_info = pipeline.run(
        redmart_source(
            app_key=APP_KEY,
            app_secret=APP_SECRET,
            access_token=ACCESS_TOKEN,
            refresh_token=REFRESH_TOKEN,
            store_id=STORE_ID,
        )
    )
    if hasattr(load_info, "load_packages") and load_info.load_packages:
        for package in load_info.load_packages:
            if hasattr(package, "schema_update"):
                print(f"\nSchema: {package.schema.name}")
                for table_name in package.schema.tables:
                    print(f"  - {table_name}")


if __name__ == "__main__":
    # Run with duckdb - data.duckdb, sell_data schema, products table
    run_pipeline()
