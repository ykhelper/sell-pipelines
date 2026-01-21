"""Test Redmart Product API using httpx."""

import hashlib
import hmac
import os
import time

import httpx
from dotenv import load_dotenv

load_dotenv()

# Credentials
APP_KEY = os.getenv("REDMART_APP_KEY")
APP_SECRET = os.getenv("REDMART_APP_SECRET")
ACCESS_TOKEN = os.getenv("REDMART_ACCESS_TOKEN")
STORE_ID = os.getenv("REDMART_STORE_ID")

# API Configuration
BASE_URL = "https://api.lazada.sg/rest"
API_PATH = "/rss/products/get"


def generate_signature(app_secret: str, api_path: str, parameters: dict) -> str:
    """Generate HMAC-SHA256 signature for Lazada/Redmart API.

    Process:
    1. Sort parameters by key (alphabetically)
    2. Concatenate: api_path + key1 + value1 + key2 + value2 + ...
    3. HMAC-SHA256 with app_secret as key
    4. Convert to uppercase hex
    """
    sorted_keys = sorted(parameters.keys())
    params_string = "".join(f"{key}{parameters[key]}" for key in sorted_keys)
    string_to_sign = f"{api_path}{params_string}"

    signature = hmac.new(
        key=app_secret.encode("utf-8"),
        msg=string_to_sign.encode("utf-8"),
        digestmod=hashlib.sha256,
    )

    return signature.hexdigest().upper()


def get_timestamp() -> str:
    """Get current timestamp in milliseconds."""
    return str(int(time.time() * 1000))


def get_products(page: int = 1, page_size: int = 100) -> dict:
    """Fetch products from Redmart API.

    Args:
        page: Page number (starts at 1)
        page_size: Number of products per page (max 100)

    Returns:
        API response as dict
    """
    # Build parameters
    params = {
        "app_key": APP_KEY,
        "sign_method": "sha256",
        "timestamp": get_timestamp(),
        "access_token": ACCESS_TOKEN,
        "storeId": STORE_ID,
        "pageSize": str(page_size),
        "page": str(page),
    }

    # Generate signature
    signature = generate_signature(APP_SECRET, API_PATH, params)
    params["sign"] = signature

    # Make request
    url = f"{BASE_URL}{API_PATH}"

    with httpx.Client() as client:
        response = client.get(url, params=params)
        return response.json()


def main():
    """Test the Redmart product API."""
    print("Testing Redmart Product API")
    print("=" * 50)

    # Check credentials
    if not all([APP_KEY, APP_SECRET, ACCESS_TOKEN, STORE_ID]):
        print("Error: Missing credentials in .env file")
        print(f"  APP_KEY: {'set' if APP_KEY else 'missing'}")
        print(f"  APP_SECRET: {'set' if APP_SECRET else 'missing'}")
        print(f"  ACCESS_TOKEN: {'set' if ACCESS_TOKEN else 'missing'}")
        print(f"  STORE_ID: {'set' if STORE_ID else 'missing'}")
        return

    print(f"Store ID: {STORE_ID}")
    print()

    # Fetch products
    print("Fetching products (page 1)...")
    result = get_products(page=1, page_size=10)

    # Print response
    print(result)
    # else:
    #     print(f"Error: {result.get('message', 'Unknown error')}")
    #     print(f"Full response: {result}")


if __name__ == "__main__":
    main()
