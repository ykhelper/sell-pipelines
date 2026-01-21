"""Generate Redmart Access Token using OAuth2 flow.

Redmart uses the same Lazada Open Platform OAuth flow.
"""

import os

from dotenv import load_dotenv

from lazadaclient.client import LazadaClient

load_dotenv()

APP_KEY = os.getenv("REDMART_APP_KEY")
APP_SECRET = os.getenv("REDMART_APP_SECRET")
REDIRECT_URI = os.getenv("REDMART_REDIRECT_URI", "https://google.com")

if not APP_KEY or not APP_SECRET:
    raise ValueError(
        "Missing REDMART_APP_KEY or REDMART_APP_SECRET in .env file"
    )


def step1_get_authorization_url():
    """Step 1: Generate authorization URL."""
    client = LazadaClient(APP_KEY, APP_SECRET)
    url = client.get_authorization_url(REDIRECT_URI)

    print("=== Redmart OAuth Step 1: Authorization ===")
    print("\n1. Open this URL in your browser:")
    print(f"\n{url}\n")
    print("2. Login with your Redmart seller account")
    print("3. Authorize the application")
    print(f"4. You will be redirected to: {REDIRECT_URI}?code=XXX")
    print("5. Copy the 'code' parameter from the URL")
    print("\nThen run: python redmart_auth.py <your_code>")


def step2_get_access_token(authorization_code: str):
    """Step 2: Exchange authorization code for access token."""
    client = LazadaClient(APP_KEY, APP_SECRET)
    response = client.get_access_token(authorization_code)

    if "access_token" in response:
        print("\n=== Redmart OAuth Step 2: Success! ===\n")
        print(f"Access Token: {response['access_token']}")
        print(f"Refresh Token: {response['refresh_token']}")
        print(f"Expires in: {response['expires_in']} seconds")
        print("\nAdd this to your .env file:")
        print(f"REDMART_ACCESS_TOKEN={response['access_token']}")
        return response
    else:
        print("\n=== Error ===")
        print(f"Response: {response}")
        return None


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        # Step 2: Exchange code for token
        code = sys.argv[1]
        step2_get_access_token(code)
    else:
        # Step 1: Get authorization URL
        step1_get_authorization_url()
