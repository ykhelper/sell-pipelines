import os

from dotenv import load_dotenv

from lazadaclient.client import LazadaClient

load_dotenv()


APP_KEY = os.getenv("REDMART_APP_KEY")
APP_SECRET = os.getenv("REDMART_APP_SECRET")
REDIRECT_URI = os.getenv("REDMART_REDIRECT_URI", "https://google.com")
CODE = os.getenv("REDMART_CODE", "")

if not APP_KEY or not APP_SECRET:
    raise ValueError("Missing LAZADA_APP_KEY or LAZADA_APP_SECRET in .env file")


def create_authorize():
    """Bước 1: Tạo URL authorize."""
    auth = LazadaClient(APP_KEY, APP_SECRET)
    url = auth.get_authorization_url(REDIRECT_URI)

    print("=== OAuth Step 1 ===")
    print("Open in browser:")
    print(url)
    print()
    print(f"Sau khi authorize, lấy code từ: {REDIRECT_URI}?code=XXX")


def create_oauth2():
    """Bước 2: Đổi code lấy token."""
    auth = LazadaClient(APP_KEY, APP_SECRET)
    response = auth.get_access_token(CODE)

    if "access_token" in response:
        print(f"Access Token: {response['access_token']}")
        print(f"Refresh Token: {response['refresh_token']}")
        print(f"Expires in: {response['expires_in']} seconds")
        return response
    else:
        print(f"Error: {response}")
        return None


def get_product(access_token: str):
    """Bước 2: Đổi code lấy token."""
    auth = LazadaClient(APP_KEY, APP_SECRET)
    response = auth.execute(
        "/products/get",
        access_token=access_token,
    )
    print(len(response["data"]["products"]))


if __name__ == "__main__":
    create_oauth2()
