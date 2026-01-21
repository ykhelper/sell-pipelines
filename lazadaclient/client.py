# auth.py
import time
from urllib.parse import urlencode

import httpx

from .signature import generate_signature


class LazadaClient:
    """Xử lý OAuth cho Lazada."""

    REGIONS = {
        "VN": "https://api.lazada.vn/rest",
        "SG": "https://api.lazada.sg/rest",
        "MY": "https://api.lazada.com.my/rest",
        "TH": "https://api.lazada.co.th/rest",
        "PH": "https://api.lazada.com.ph/rest",
        "ID": "https://api.lazada.co.id/rest",
    }

    AUTH_URL = "https://auth.lazada.com/rest"
    AUTHORIZE_URL = "https://auth.lazada.com/oauth/authorize"

    def __init__(self, app_key: str, app_secret: str):
        self.app_key = app_key
        self.app_secret = app_secret
        self.base_url = "https://api.lazada.sg/rest"

    def get_authorization_url(self, redirect_uri: str) -> str:
        """
        Tạo URL để user authorize.

        Args:
            redirect_uri: URL callback sau khi user authorize

        Returns:
            URL để mở trong browser
        """
        params = {
            "response_type": "code",
            "client_id": self.app_key,
            "redirect_uri": redirect_uri,
            "force_auth": "true",
        }
        return f"{self.AUTHORIZE_URL}?{urlencode(params)}"

    def get_access_token(self, code: str) -> dict:
        """
        Đổi authorization code lấy access token.

        Args:
            code: Code từ callback URL sau khi user authorize

        Returns:
            Dict chứa access_token, refresh_token, expires_in, ...
        """
        api_path = "/auth/token/create"

        params = {
            "app_key": self.app_key,
            "sign_method": "sha256",
            "timestamp": str(int(time.time() * 1000)),
            "code": code,
        }

        signature = generate_signature(self.app_secret, api_path, params)
        params["sign"] = signature

        url = f"{self.AUTH_URL}{api_path}"
        response = httpx.post(url, data=params, timeout=30)

        return response.json()

    def refresh_access_token(self, refresh_token: str) -> dict:
        """
        Refresh access token khi hết hạn.

        Args:
            refresh_token: Refresh token từ lần get_access_token trước

        Returns:
            Dict chứa access_token mới
        """
        api_path = "/auth/token/refresh"

        params = {
            "app_key": self.app_key,
            "sign_method": "sha256",
            "timestamp": str(int(time.time() * 1000)),
            "refresh_token": refresh_token,
        }

        signature = generate_signature(self.app_secret, api_path, params)
        params["sign"] = signature

        url = f"{self.AUTH_URL}{api_path}"
        response = httpx.post(url, data=params, timeout=30)

        return response.json()

    def _get_timestamp(self) -> str:
        """Timestamp tính bằng milliseconds."""
        return str(int(time.time() * 1000))

    def execute(
        self,
        api_path: str,
        params: dict = None,
        access_token: str = None,
        method: str = "GET",
    ) -> dict:
        """
        Gọi Lazada API.

        Args:
            api_path: Đường dẫn API (vd: "/category/tree/get")
            params: Parameters cho API
            access_token: Token cho seller APIs
            method: "GET" hoặc "POST"

        Returns:
            JSON response từ Lazada
        """
        params = {"limit": 50, "offset": 2}

        system_params = {
            "app_key": self.app_key,
            "sign_method": "sha256",
            "timestamp": self._get_timestamp(),
        }

        if access_token:
            system_params["access_token"] = access_token

        # Gộp tất cả parameters
        all_params = {**system_params, **params}

        # Tạo signature
        signature = generate_signature(self.app_secret, api_path, all_params)
        all_params["sign"] = signature

        # Gọi API
        url = f"{self.base_url}{api_path}"

        if method.upper() == "POST":
            response = httpx.post(url, data=all_params, timeout=30)
        else:
            response = httpx.get(url, params=all_params, timeout=30)

        return response.json()
