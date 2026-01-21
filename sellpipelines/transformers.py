"""Field extraction and transformation functions for e-commerce platforms.

This module consolidates data transformation logic for all platforms,
ensuring consistent schema across Shopee, Redmart, and Lazada data.
"""

from typing import Any, Dict


def extract_shopee_fields(product: Dict[str, Any]) -> Dict[str, Any]:
    """Extract and normalize fields from a Shopee product.

    Args:
        product: Raw product data from Shopee API

    Returns:
        Normalized product record with unified schema
    """
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
        # Keep original item_id for reference
        "item_id": product.get("item_id"),
    }


def extract_redmart_fields(product: Dict[str, Any]) -> Dict[str, Any]:
    """Extract and normalize fields from a Redmart product.

    Args:
        product: Raw product data from Redmart API

    Returns:
        Normalized product record with unified schema

    Note:
        The Redmart API (/rss/products/get) does not return image_url or stock
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
        # Keep original rpc for reference
        "rpc": product.get("rpc"),
    }


def extract_lazada_fields(product: Dict[str, Any]) -> Dict[str, Any]:
    """Extract and normalize fields from a Lazada product.

    Args:
        product: Raw product data from Lazada API

    Returns:
        Normalized product record with unified schema
    """
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
        # Keep original item_id for reference
        "item_id": product.get("item_id"),
    }
