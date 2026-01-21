"""Lazada API Pipeline using DLT REST API Source.

This pipeline fetches product data from Lazada Seller API (Singapore) using:
- Custom signature-based authentication
- Offset-based pagination
- duckdb as destination
"""

import os
from typing import Any, Iterator, Optional

import dlt
from dlt.sources.helpers.rest_client import paginate
from dlt.sources.helpers.rest_client.paginators import OffsetPaginator
from dotenv import load_dotenv

from lazadaclient.auth import LazadaAuth

# Load environment variables
load_dotenv()

# Required credentials
APP_KEY = os.getenv("LAZADA_APP_KEY")
APP_SECRET = os.getenv("LAZADA_APP_SECRET")
ACCESS_TOKEN = os.getenv("LAZADA_ACCESS_TOKEN")
REFRESH_TOKEN = os.getenv("LAZADA_REFRESH_TOKEN")

if not APP_KEY or not APP_SECRET:
    raise ValueError("Missing LAZADA_APP_KEY or LAZADA_APP_SECRET in .env file")

if not ACCESS_TOKEN:
    raise ValueError(
        "Missing LAZADA_ACCESS_TOKEN in .env file. "
        "Use test.py to obtain an access token first."
    )

# Note: REFRESH_TOKEN is optional but recommended for auto-refresh
if not REFRESH_TOKEN:
    print(
        "Warning: LAZADA_REFRESH_TOKEN not set. "
        "Auto-refresh will not work when token expires."
    )


def extract_lazada_fields(product: dict) -> dict:
    """Extract only the required fields from a Lazada product."""
    # Get first SKU for stock, image, and barcode info
    skus = product.get("skus", [])
    first_sku = skus[0] if skus else {}

    # Get images from first SKU
    images = first_sku.get("Images", [])
    first_image = next((img for img in images if img), None)

    return {
        "platform_id": str(product.get("item_id")),
        "product_name": product.get("attributes", {}).get("name"),
        "barcode": first_sku.get("SellerSku"),
        "image_url": first_image,
        "stock": first_sku.get("quantity"),
        "store_id": "lazada",
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
) -> Iterator[Any]:
    """
    Fetch products from Lazada API with pagination.

    Args:
        app_key: Lazada application key
        app_secret: Lazada application secret
        access_token: OAuth access token for seller APIs
        refresh_token: OAuth refresh token for auto-refresh

    Yields:
        Product records with only: item_id, product_name, barcode, image_url, stock
    """
    # Base URL and endpoint
    base_url = "https://api.lazada.sg/rest"
    endpoint = "/products/get"

    # Create authentication with auto-refresh support
    auth = LazadaAuth(
        app_key=app_key,
        app_secret=app_secret,
        access_token=access_token,
        refresh_token=refresh_token,
        platform="lazada",
    )

    # Configure paginator
    paginator = OffsetPaginator(
        limit=100,
        offset=0,
        offset_param="offset",
        limit_param="limit",
        total_path="data.total_products",
    )

    # Paginate through all products
    for page in paginate(
        url=f"{base_url}{endpoint}",
        auth=auth,
        paginator=paginator,
        params={
            "limit": 100,
            "offset": 0,
        },
    ):
        # Extract products from response
        if (
            isinstance(page, dict)
            and "data" in page
            and "products" in page["data"]
        ):
            products = page["data"]["products"]
            if isinstance(products, list):
                for product in products:
                    yield extract_lazada_fields(product)
            else:
                yield extract_lazada_fields(products)
        else:
            # If page is already a product dict
            yield extract_lazada_fields(page)


@dlt.source
def lazada_source(
    app_key: str = dlt.secrets.value,
    app_secret: str = dlt.secrets.value,
    access_token: str = dlt.secrets.value,
    refresh_token: Optional[str] = None,
):
    """
    DLT source for Lazada Seller API (Singapore).

    Args:
        app_key: Lazada application key
        app_secret: Lazada application secret
        access_token: OAuth access token for seller APIs
        refresh_token: OAuth refresh token for auto-refresh

    Returns:
        DLT source with products resource
    """
    return get_products(
        app_key=app_key,
        app_secret=app_secret,
        access_token=access_token,
        refresh_token=refresh_token,
    )


def run_pipeline(
    destination: str = "duckdb",
    dataset_name: str = "sell_data",
    dev_mode: bool = False,
) -> None:
    """
    Run the Lazada pipeline.

    Args:
        destination: DLT destination
        dataset_name: Name of the dataset
        dev_mode: Enable dev mode for development (resets schema/state)
    """
    # Create pipeline - use data.duckdb as the shared database
    pipeline = dlt.pipeline(
        pipeline_name="lazada_pipeline",
        destination=dlt.destinations.duckdb("data.duckdb"),
        dataset_name=dataset_name,
        dev_mode=dev_mode,
        progress="log",  # Show progress in logs
    )

    # Load data
    load_info = pipeline.run(
        lazada_source(
            app_key=APP_KEY,
            app_secret=APP_SECRET,
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
