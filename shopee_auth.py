"""Generate Shopee Access Token using OAuth2 flow.

Based on the existing ShopeeClient implementation.
"""

import os

from dotenv import load_dotenv

from shopeeclient.client import ShopeeClient

load_dotenv()

PARTNER_ID = os.getenv("SHOPEE_APP_ID")
PARTNER_KEY = os.getenv("SHOPEE_APP_KEY")
SHOP_ID = os.getenv("SHOPEE_SHOP_ID")
CODE = os.getenv("SHOPEE_CODE")

if not PARTNER_ID or not PARTNER_KEY:
    raise ValueError("Missing SHOPEE_APP_ID or SHOPEE_APP_KEY in .env file")

if not SHOP_ID:
    raise ValueError("Missing SHOPEE_SHOP_ID in .env file")


def step1_get_authorization_url():
    """Step 1: Generate authorization URL."""
    client = ShopeeClient(
        partner_id=int(PARTNER_ID),
        partner_key=PARTNER_KEY,
        code=CODE or "",
        shop_id=SHOP_ID,
    )
    url = client.generateAuthorize()

    print("=== Shopee OAuth Step 1: Authorization ===")
    print("\n1. Open this URL in your browser:")
    print(f"\n{url}\n")
    print("2. Login with your Shopee seller account")
    print("3. Authorize the application")
    print(
        "4. You will be redirected to: https://google.com?code=XXX&shop_id=XXX"
    )
    print("5. Copy the 'code' parameter from the URL")
    print("\nThen add to .env file:")
    print("SHOPEE_CODE=<your_code>")
    print("\nAnd run: python shopee_auth.py token")


def step2_get_access_token():
    """Step 2: Exchange authorization code for access token."""
    if not CODE:
        print("ERROR: SHOPEE_CODE not found in .env file")
        print("Please run: python shopee_auth.py")
        return

    client = ShopeeClient(
        partner_id=int(PARTNER_ID),
        partner_key=PARTNER_KEY,
        code=CODE,
        shop_id=SHOP_ID,
    )
    response = client.get_access_token()

    if "access_token" in response:
        print("\n=== Shopee OAuth Step 2: Success! ===\n")
        print(f"Access Token: {response['access_token']}")
        print(f"Refresh Token: {response['refresh_token']}")
        print(f"Expires in: {response.get('expire_in', 'N/A')} seconds")
        print("\nAdd this to your .env file:")
        print(f"SHOPEE_ACCESS_TOKEN={response['access_token']}")
        print(f"SHOPEE_REFRESH_TOKEN={response['refresh_token']}")
        return response
    else:
        print("\n=== Error ===")
        print(f"Response: {response}")
        return None


def step3_refresh_token():
    """Step 3: Refresh access token using refresh token."""
    refresh_token = os.getenv("SHOPEE_REFRESH_TOKEN")

    if not refresh_token:
        print("ERROR: SHOPEE_REFRESH_TOKEN not found in .env file")
        return

    client = ShopeeClient(
        partner_id=int(PARTNER_ID),
        partner_key=PARTNER_KEY,
        code=CODE or "",
        shop_id=int(SHOP_ID),
    )
    access_token, new_refresh_token = client.refreshToken(refresh_token)
    if access_token:
        print("\n=== Shopee Token Refresh: Success! ===\n")
        print(f"New Access Token: {access_token}")
        print(f"New Refresh Token: {new_refresh_token}")
        print("\nUpdate these in your .env file:")
        print(f"SHOPEE_ACCESS_TOKEN={access_token}")
        print(f"SHOPEE_REFRESH_TOKEN={new_refresh_token}")
    else:
        print("\n=== Error refreshing token ===")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        if command == "token":
            # Step 2: Exchange code for token
            step2_get_access_token()
        elif command == "refresh":
            # Step 3: Refresh token
            step3_refresh_token()
        else:
            print("Unknown command. Usage:")
            print("  python shopee_auth.py         # Get authorization URL")
            print("  python shopee_auth.py token   # Exchange code for token")
            print("  python shopee_auth.py refresh # Refresh access token")
    else:
        # Step 1: Get authorization URL
        step1_get_authorization_url()
