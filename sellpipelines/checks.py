"""Asset checks for e-commerce data quality validation.

This module defines asset checks that verify:
- Products table is not empty
- Required fields are populated
- Data integrity constraints
"""

from pathlib import Path

import duckdb
from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
    AssetKey,
    asset_check,
)

from sellpipelines.assets import DUCKDB_PATH


def _get_product_count(platform: str) -> int:
    """Get the count of products for a platform from DuckDB."""
    if not Path(DUCKDB_PATH).exists():
        return 0

    try:
        conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)
        result = conn.execute(
            """
            SELECT COUNT(*)
            FROM sell_data.products
            WHERE store_id = ?
            """,
            [platform],
        ).fetchone()
        conn.close()
        return result[0] if result else 0
    except Exception:
        return 0


def _get_null_field_count(platform: str, field: str) -> int:
    """Get count of null values for a field in a platform's products."""
    if not Path(DUCKDB_PATH).exists():
        return 0

    try:
        conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)
        result = conn.execute(
            f"""
            SELECT COUNT(*)
            FROM sell_data.products
            WHERE store_id = ? AND {field} IS NULL
            """,
            [platform],
        ).fetchone()
        conn.close()
        return result[0] if result else 0
    except Exception:
        return 0


# =============================================================================
# SHOPEE CHECKS
# =============================================================================


@asset_check(
    asset=AssetKey("dlt_shopee_source_products"),
    description="Check that Shopee products were loaded",
)
def shopee_products_not_empty(
    context: AssetCheckExecutionContext,
) -> AssetCheckResult:
    """Verify that Shopee products table contains data."""
    count = _get_product_count("shopee")

    if count == 0:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            metadata={"product_count": count},
            description="No Shopee products found in database",
        )

    return AssetCheckResult(
        passed=True,
        metadata={"product_count": count},
        description=f"Found {count} Shopee products",
    )


@asset_check(
    asset=AssetKey("dlt_shopee_source_products"),
    description="Check that Shopee products have required fields",
)
def shopee_products_have_required_fields(
    context: AssetCheckExecutionContext,
) -> AssetCheckResult:
    """Verify that Shopee products have platform_id and product_name."""
    total = _get_product_count("shopee")
    if total == 0:
        return AssetCheckResult(
            passed=True,
            metadata={"skipped": True},
            description="No products to check",
        )

    null_platform_id = _get_null_field_count("shopee", "platform_id")
    null_product_name = _get_null_field_count("shopee", "product_name")

    issues = []
    if null_platform_id > 0:
        issues.append(f"{null_platform_id} products missing platform_id")
    if null_product_name > 0:
        issues.append(f"{null_product_name} products missing product_name")

    if issues:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            metadata={
                "null_platform_id": null_platform_id,
                "null_product_name": null_product_name,
            },
            description="; ".join(issues),
        )

    return AssetCheckResult(
        passed=True,
        metadata={"total_products": total},
        description="All required fields are populated",
    )


# =============================================================================
# REDMART CHECKS
# =============================================================================


@asset_check(
    asset=AssetKey("dlt_redmart_source_products"),
    description="Check that Redmart products were loaded",
)
def redmart_products_not_empty(
    context: AssetCheckExecutionContext,
) -> AssetCheckResult:
    """Verify that Redmart products table contains data."""
    count = _get_product_count("redmart")

    if count == 0:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            metadata={"product_count": count},
            description="No Redmart products found in database",
        )

    return AssetCheckResult(
        passed=True,
        metadata={"product_count": count},
        description=f"Found {count} Redmart products",
    )


@asset_check(
    asset=AssetKey("dlt_redmart_source_products"),
    description="Check that Redmart products have required fields",
)
def redmart_products_have_required_fields(
    context: AssetCheckExecutionContext,
) -> AssetCheckResult:
    """Verify that Redmart products have platform_id and product_name."""
    total = _get_product_count("redmart")
    if total == 0:
        return AssetCheckResult(
            passed=True,
            metadata={"skipped": True},
            description="No products to check",
        )

    null_platform_id = _get_null_field_count("redmart", "platform_id")
    null_product_name = _get_null_field_count("redmart", "product_name")

    issues = []
    if null_platform_id > 0:
        issues.append(f"{null_platform_id} products missing platform_id")
    if null_product_name > 0:
        issues.append(f"{null_product_name} products missing product_name")

    if issues:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            metadata={
                "null_platform_id": null_platform_id,
                "null_product_name": null_product_name,
            },
            description="; ".join(issues),
        )

    return AssetCheckResult(
        passed=True,
        metadata={"total_products": total},
        description="All required fields are populated",
    )


# =============================================================================
# LAZADA CHECKS
# =============================================================================


@asset_check(
    asset=AssetKey("dlt_lazada_source_products"),
    description="Check that Lazada products were loaded",
)
def lazada_products_not_empty(
    context: AssetCheckExecutionContext,
) -> AssetCheckResult:
    """Verify that Lazada products table contains data."""
    count = _get_product_count("lazada")

    if count == 0:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            metadata={"product_count": count},
            description="No Lazada products found in database",
        )

    return AssetCheckResult(
        passed=True,
        metadata={"product_count": count},
        description=f"Found {count} Lazada products",
    )


@asset_check(
    asset=AssetKey("dlt_lazada_source_products"),
    description="Check that Lazada products have required fields",
)
def lazada_products_have_required_fields(
    context: AssetCheckExecutionContext,
) -> AssetCheckResult:
    """Verify that Lazada products have platform_id and product_name."""
    total = _get_product_count("lazada")
    if total == 0:
        return AssetCheckResult(
            passed=True,
            metadata={"skipped": True},
            description="No products to check",
        )

    null_platform_id = _get_null_field_count("lazada", "platform_id")
    null_product_name = _get_null_field_count("lazada", "product_name")

    issues = []
    if null_platform_id > 0:
        issues.append(f"{null_platform_id} products missing platform_id")
    if null_product_name > 0:
        issues.append(f"{null_product_name} products missing product_name")

    if issues:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            metadata={
                "null_platform_id": null_platform_id,
                "null_product_name": null_product_name,
            },
            description="; ".join(issues),
        )

    return AssetCheckResult(
        passed=True,
        metadata={"total_products": total},
        description="All required fields are populated",
    )


# Export all checks
ALL_CHECKS = [
    shopee_products_not_empty,
    shopee_products_have_required_fields,
    redmart_products_not_empty,
    redmart_products_have_required_fields,
    lazada_products_not_empty,
    lazada_products_have_required_fields,
]
