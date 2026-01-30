"""Generate Lazada Access Token using OAuth2 flow.

Lazada uses the Lazada Open Platform OAuth flow.
"""

import os

from dotenv import load_dotenv

from lazadaclient.client import LazadaClient

load_dotenv()

APP_KEY = os.getenv("LAZADA_APP_KEY")
APP_SECRET = os.getenv("LAZADA_APP_SECRET")
REDIRECT_URI = os.getenv("LAZADA_REDIRECT_URI", "https://google.com")
CODE = os.getenv("LAZADA_CODE", "")

if not APP_KEY or not APP_SECRET:
    raise ValueError("Missing LAZADA_APP_KEY or LAZADA_APP_SECRET in .env file")


def step1_get_authorization_url():
    """Step 1: Generate authorization URL."""
    client = LazadaClient(APP_KEY, APP_SECRET)
    url = client.get_authorization_url(REDIRECT_URI)

    print("=== Lazada OAuth Step 1: Authorization ===")
    print("\n1. Open this URL in your browser:")
    print(f"\n{url}\n")
    print("2. Login with your Lazada seller account")
    print("3. Authorize the application")
    print(f"4. You will be redirected to: {REDIRECT_URI}?code=XXX")
    print("5. Copy the 'code' parameter from the URL")
    print("\nThen run: python lazada_auth.py <your_code>")


def step2_get_access_token():
    """Step 2: Exchange authorization code for access token."""
    client = LazadaClient(APP_KEY, APP_SECRET)

    try:
        print("\n=== Debug: Attempting to get access token ===")
        print(f"App Key: {APP_KEY[:10]}...")

        response = client.get_access_token(CODE)

        if "access_token" in response:
            print("\n=== Lazada OAuth Step 2: Success! ===\n")
            print(f"Access Token: {response['access_token']}")
            print(f"Refresh Token: {response['refresh_token']}")
            print(f"Expires in: {response['expires_in']} seconds")
            print("\nAdd this to your .env file:")
            print(f"LAZADA_ACCESS_TOKEN={response['access_token']}")
            print(f"LAZADA_REFRESH_TOKEN={response['refresh_token']}")
            return response
        else:
            print("\n=== Error ===")
            print(f"Full Response: {response}")
            return None
    except Exception as e:
        print("\n=== DETAILED ERROR ===")
        print(f"Error Type: {type(e).__name__}")
        print(f"Error Message: {str(e)}")
        print("\nFull Exception Details:")
        import traceback

        traceback.print_exc()

        # Try to extract response details if available
        if hasattr(e, "response"):
            print("\n=== API Response Details ===")
            print(
                f"Status Code: {e.response.status_code if hasattr(e.response, 'status_code') else 'N/A'}"
            )
            print(
                f"Response Text: {e.response.text if hasattr(e.response, 'text') else 'N/A'}"
            )

        # Check if there's additional error info from the client
        if hasattr(e, "__dict__"):
            print(f"\nException attributes: {e.__dict__}")
        return None


def step3_refresh_token():
    """Step 3: Refresh access token using refresh token."""
    refresh_token = os.getenv("LAZADA_REFRESH_TOKEN")

    if not refresh_token:
        print("ERROR: LAZADA_REFRESH_TOKEN not found in .env file")
        return

    client = LazadaClient(APP_KEY, APP_SECRET)

    try:
        print("\n=== Debug: Attempting to refresh token ===")
        print(f"App Key: {APP_KEY[:10]}...")
        print(f"Refresh Token (first 20 chars): {refresh_token[:20]}...")

        response = client.refresh_access_token(refresh_token)

        if "access_token" in response:
            print("\n=== Lazada Token Refresh: Success! ===\n")
            print(f"New Access Token: {response['access_token']}")
            print(
                f"New Refresh Token: {response.get('refresh_token', 'Not provided')}"
            )
            print(f"Expires in: {response.get('expires_in', 'N/A')} seconds")
            print("\nUpdate these in your .env file:")
            print(f"LAZADA_ACCESS_TOKEN={response['access_token']}")
            if "refresh_token" in response:
                print(f"LAZADA_REFRESH_TOKEN={response['refresh_token']}")
            return response
        else:
            print("\n=== Error ===")
            print(f"Full Response: {response}")
            return None
    except Exception as e:
        print("\n=== DETAILED ERROR ===")
        print(f"Error Type: {type(e).__name__}")
        print(f"Error Message: {str(e)}")
        print("\nFull Exception Details:")
        import traceback

        traceback.print_exc()

        # Try to extract response details if available
        if hasattr(e, "response"):
            print("\n=== API Response Details ===")
            print(
                f"Status Code: {e.response.status_code if hasattr(e.response, 'status_code') else 'N/A'}"
            )
            print(
                f"Response Text: {e.response.text if hasattr(e.response, 'text') else 'N/A'}"
            )

        # Check if there's additional error info from the client
        if hasattr(e, "__dict__"):
            print(f"\nException attributes: {e.__dict__}")
        return None


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        command = sys.argv[1]
        if command.lower() == "refresh":
            # Step 3: Refresh token
            step3_refresh_token()
        else:
            # Step 2: Exchange code for token (backward compatibility)
            step2_get_access_token()
    else:
        # Step 1: Get authorization URL
        step1_get_authorization_url()
