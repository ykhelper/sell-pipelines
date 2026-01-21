"""Dagster assets for e-commerce platform data pipelines.

This module defines dlt assets for:
- Shopee products
- Redmart products
- Lazada products

With features:
- Retry policies for API failures
- Metadata and tags for observability
- Consistent DuckDB destination
- Token refresh support
"""

import os
from pathlib import Path

import dlt
from dagster import AssetExecutionContext, MetadataValue, RetryPolicy
from dagster_dlt import DagsterDltResource, dlt_assets
from dotenv import load_dotenv

from sellpipelines.sources import lazada_source, redmart_source, shopee_source

# Load environment variables
load_dotenv()

# DuckDB path - consistent across all pipelines
DUCKDB_PATH = Path(__file__).parent.parent / "data.duckdb"


def _get_env(key: str, default: str = "") -> str:
    """Get environment variable with default."""
    return os.getenv(key, default)


def _get_env_int(key: str, default: int = 0) -> int:
    """Get environment variable as int with default."""
    val = os.getenv(key)
    return int(val) if val else default


# Shopee credentials
SHOPEE_PARTNER_ID = _get_env_int("SHOPEE_APP_ID")
SHOPEE_PARTNER_KEY = _get_env("SHOPEE_APP_KEY")
SHOPEE_SHOP_ID = _get_env_int("SHOPEE_SHOP_ID")
SHOPEE_ACCESS_TOKEN = _get_env("SHOPEE_ACCESS_TOKEN")
SHOPEE_REFRESH_TOKEN = _get_env("SHOPEE_REFRESH_TOKEN")

# Redmart credentials
REDMART_APP_KEY = _get_env("REDMART_APP_KEY")
REDMART_APP_SECRET = _get_env("REDMART_APP_SECRET")
REDMART_ACCESS_TOKEN = _get_env("REDMART_ACCESS_TOKEN")
REDMART_REFRESH_TOKEN = _get_env("REDMART_REFRESH_TOKEN")
REDMART_STORE_ID = _get_env("REDMART_STORE_ID")

# Lazada credentials
LAZADA_APP_KEY = _get_env("LAZADA_APP_KEY")
LAZADA_APP_SECRET = _get_env("LAZADA_APP_SECRET")
LAZADA_ACCESS_TOKEN = _get_env("LAZADA_ACCESS_TOKEN")
LAZADA_REFRESH_TOKEN = _get_env("LAZADA_REFRESH_TOKEN")

# Retry policy for API failures
API_RETRY_POLICY = RetryPolicy(
    max_retries=3,
    delay=60,  # 60 seconds between retries
)


@dlt_assets(
    dlt_source=shopee_source(
        partner_id=SHOPEE_PARTNER_ID,
        partner_key=SHOPEE_PARTNER_KEY,
        shop_id=SHOPEE_SHOP_ID,
        access_token=SHOPEE_ACCESS_TOKEN,
        refresh_token=SHOPEE_REFRESH_TOKEN or None,
    ),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="shopee_pipeline",
        dataset_name="sell_data",
        destination=dlt.destinations.duckdb(str(DUCKDB_PATH)),
        progress="log",
    ),
    name="shopee",
    group_name="ecommerce",
)
def shopee_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    """Load Shopee product data into DuckDB.

    Fetches products from Shopee Open Platform API and loads them into
    the sell_data schema in DuckDB with normalized field structure.
    """
    for result in dlt.run(context=context):
        # Add metadata about the load
        context.add_output_metadata(
            metadata={
                "platform": MetadataValue.text("Shopee"),
                "shop_id": MetadataValue.text(str(SHOPEE_SHOP_ID)),
                "database": MetadataValue.path(str(DUCKDB_PATH)),
            }
        )
        yield result


@dlt_assets(
    dlt_source=redmart_source(
        app_key=REDMART_APP_KEY,
        app_secret=REDMART_APP_SECRET,
        access_token=REDMART_ACCESS_TOKEN,
        store_id=REDMART_STORE_ID,
        refresh_token=REDMART_REFRESH_TOKEN or None,
    ),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="redmart_pipeline",
        dataset_name="sell_data",
        destination=dlt.destinations.duckdb(str(DUCKDB_PATH)),
        progress="log",
    ),
    name="redmart",
    group_name="ecommerce",
)
def redmart_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    """Load Redmart product data into DuckDB.

    Fetches products from Redmart API (via Lazada Open Platform) and loads them
    into the sell_data schema in DuckDB with normalized field structure.

    Note: Redmart API does not provide image_url or stock information.
    """
    for result in dlt.run(context=context):
        context.add_output_metadata(
            metadata={
                "platform": MetadataValue.text("Redmart"),
                "store_id": MetadataValue.text(str(REDMART_STORE_ID)),
                "database": MetadataValue.path(str(DUCKDB_PATH)),
            }
        )
        yield result


@dlt_assets(
    dlt_source=lazada_source(
        app_key=LAZADA_APP_KEY,
        app_secret=LAZADA_APP_SECRET,
        access_token=LAZADA_ACCESS_TOKEN,
        refresh_token=LAZADA_REFRESH_TOKEN or None,
    ),
    dlt_pipeline=dlt.pipeline(
        pipeline_name="lazada_pipeline",
        dataset_name="sell_data",
        destination=dlt.destinations.duckdb(str(DUCKDB_PATH)),
        progress="log",
    ),
    name="lazada",
    group_name="ecommerce",
)
def lazada_assets(context: AssetExecutionContext, dlt: DagsterDltResource):
    """Load Lazada product data into DuckDB.

    Fetches products from Lazada Seller API (Singapore) and loads them
    into the sell_data schema in DuckDB with normalized field structure.
    """
    for result in dlt.run(context=context):
        context.add_output_metadata(
            metadata={
                "platform": MetadataValue.text("Lazada"),
                "database": MetadataValue.path(str(DUCKDB_PATH)),
            }
        )
        yield result
