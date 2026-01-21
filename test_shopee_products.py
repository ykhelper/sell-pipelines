"""Test Shopee API product data using pure httpx."""

import hashlib
import hmac
import os
import time

import httpx
from dotenv import load_dotenv

load_dotenv()

# Credentials
PARTNER_ID = int(os.getenv("SHOPEE_APP_ID", "0"))
PARTNER_KEY = os.getenv("SHOPEE_APP_KEY", "")
SHOP_ID = int(os.getenv("SHOPEE_SHOP_ID", "0"))
ACCESS_TOKEN = os.getenv("SHOPEE_ACCESS_TOKEN", "")

BASE_URL = "https://partner.shopeemobile.com"


def generate_signature(path: str, timestamp: int) -> str:
    """Generate HMAC-SHA256 signature for Shopee API."""
    base_string = f"{PARTNER_ID}{path}{timestamp}{ACCESS_TOKEN}{SHOP_ID}"
    return hmac.new(
        PARTNER_KEY.encode("utf-8"),
        base_string.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def build_url(path: str, params: dict | None = None) -> str:
    """Build authenticated URL for Shopee API."""
    timestamp = int(time.time())
    signature = generate_signature(path, timestamp)

    auth_params = {
        "partner_id": PARTNER_ID,
        "shop_id": SHOP_ID,
        "access_token": ACCESS_TOKEN,
        "timestamp": timestamp,
        "sign": signature,
    }

    if params:
        auth_params.update(params)

    query_string = "&".join(f"{k}={v}" for k, v in auth_params.items())
    return f"{BASE_URL}{path}?{query_string}"


def get_item_list(offset: int = 0, page_size: int = 50) -> dict:
    """Fetch item list from Shopee API."""
    path = "/api/v2/product/get_item_list"
    params = {
        "offset": offset,
        "page_size": page_size,
        "item_status": "NORMAL",
    }

    url = build_url(path, params)
    response = httpx.get(url)
    return response.json()


def get_item_base_info(item_ids: list[int]) -> dict:
    """Fetch item base info from Shopee API."""
    path = "/api/v2/product/get_item_base_info"
    params = {
        "item_id_list": ",".join(str(id) for id in item_ids),
        "need_tax_info": "false",
        "need_complaint_policy": "false",
    }

    url = build_url(path, params)
    response = httpx.get(url)
    return response.json()


def extract_product_fields(product: dict) -> dict:
    """Extract required fields from product data."""
    image_data = product.get("image", {})
    image_url_list = image_data.get("image_url_list", [])
    first_image = image_url_list[0] if image_url_list else None

    stock_info = product.get("stock_info_v2", {})
    seller_stock = stock_info.get("seller_stock", [])
    stock = (
        seller_stock[0].get("stock")
        if seller_stock
        else stock_info.get("total_available_stock")
    )

    barcode = product.get("gtin_code")
    if not barcode or barcode == "00":
        barcode = product.get("item_sku")

    return {
        "platform_id": str(product.get("item_id")),
        "product_name": product.get("item_name"),
        "barcode": barcode,
        "image_url": first_image,
        "stock": stock,
        "store_id": "shopee",
    }


def test_get_item_list():
    """Test fetching item list."""
    print("\n=== Testing get_item_list ===")

    data = get_item_list(offset=0, page_size=10)

    if "error" in data and data.get("error"):
        print(f"Error: {data.get('error')} - {data.get('message')}")
        return []

    response = data.get("response", {})
    items = response.get("item", [])
    has_next = response.get("has_next_page", False)
    total = response.get("total_count", 0)

    print(f"Total items: {total}")
    print(f"Items fetched: {len(items)}")
    print(f"Has next page: {has_next}")

    if items:
        print("\nFirst few item IDs:")
        for item in items[:5]:
            print(
                f"  - {item.get('item_id')}: status={item.get('item_status')}"
            )

    return [item.get("item_id") for item in items]


def test_get_item_base_info(item_ids: list[int]):
    """Test fetching item base info."""
    print("\n=== Testing get_item_base_info ===")

    if not item_ids:
        print("No item IDs to fetch")
        return

    # Test with first 5 items
    test_ids = item_ids[:5]
    print(f"Fetching info for {len(test_ids)} items: {test_ids}")

    data = get_item_base_info(test_ids)

    if "error" in data and data.get("error"):
        print(f"Error: {data.get('error')} - {data.get('message')}")
        return

    response = data.get("response", {})
    item_list = response.get("item_list", [])

    print(f"Items returned: {len(item_list)}")

    for product in item_list:
        extracted = extract_product_fields(product)
        print(f"\nProduct: {extracted['product_name']}")
        print(f"  Platform ID: {extracted['platform_id']}")
        print(f"  Barcode: {extracted['barcode']}")
        print(f"  Stock: {extracted['stock']}")
        print(
            f"  Image: {extracted['image_url'][:50] if extracted['image_url'] else 'None'}..."
        )


def test_raw_product_response(item_ids: list[int]):
    """Print raw product response for inspection."""
    print("\n=== Raw Product Response ===")

    if not item_ids:
        print("No item IDs to fetch")
        return

    data = get_item_base_info(item_ids[:1])

    if "error" in data and data.get("error"):
        print(f"Error: {data.get('error')} - {data.get('message')}")
        return

    response = data.get("response", {})
    item_list = response.get("item_list", [])

    if item_list:
        import json

        print(json.dumps(item_list[0], indent=2, ensure_ascii=False))


def test_pagination():
    """Test pagination through all items."""
    print("\n=== Testing Pagination ===")

    all_ids = []
    offset = 0
    page_size = 50

    while True:
        data = get_item_list(offset=offset, page_size=page_size)

        if "error" in data and data.get("error"):
            print(f"Error at offset {offset}: {data.get('message')}")
            break

        response = data.get("response", {})
        items = response.get("item", [])
        has_next = response.get("has_next_page", False)

        all_ids.extend(item.get("item_id") for item in items)
        print(f"Offset {offset}: fetched {len(items)} items")

        if not has_next:
            break

        offset = response.get("next_offset", offset + page_size)

    print(f"\nTotal items fetched: {len(all_ids)}")
    return all_ids


if __name__ == "__main__":
    print("Shopee API Product Data Test")
    print("=" * 50)
    print(f"Partner ID: {PARTNER_ID}")
    print(f"Shop ID: {SHOP_ID}")
    print(
        f"Access Token: {ACCESS_TOKEN[:20]}..." if ACCESS_TOKEN else "No token"
    )

    # Test item list
    item_ids = test_get_item_list()

    # Test item base info
    if item_ids:
        test_get_item_base_info(item_ids)
        test_raw_product_response(item_ids)

    # Uncomment to test full pagination
    # test_pagination()
