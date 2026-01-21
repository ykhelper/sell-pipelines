import hashlib
import hmac
import json
import time
from datetime import datetime

import httpx


class ShopeeClient:
    """Handle Shopee API authentication."""

    def __init__(
        self,
        partner_id: int,
        partner_key: str,
        code: str,
        shop_id: int,
        host: str = "https://partner.shopeemobile.com",
    ):
        """
        Initialize ShopeeAuth.

        Args:
            partner_id: Partner ID from Shopee
            partner_key: Partner key from Shopee
            host: API host URL
        """
        self.partner_id = partner_id
        self.partner_key = partner_key
        self.code = code
        self.shop_id = shop_id
        self.host = host

    def generateAuthorize(self):
        timest = int(time.time())
        path = "/api/v2/shop/auth_partner"
        redirect_url = "https://google.com"
        tmp_base_string = "%s%s%s" % (self.partner_id, path, timest)
        base_string = tmp_base_string.encode()
        sign = hmac.new(
            self.partner_key.encode("utf-8"), base_string, hashlib.sha256
        ).hexdigest()
        ##generate api
        url = (
            self.host
            + path
            + "?partner_id=%s&timestamp=%s&sign=%s&redirect=%s"
            % (self.partner_id, timest, sign, redirect_url)
        )
        return url

    def get_access_token(self):
        path = "/api/v2/auth/token/get"
        timestamp = int(time.time())

        body = {
            "code": self.code,
            "shop_id": self.shop_id,
            "partner_id": self.partner_id,
        }
        baseStr = str(self.partner_id) + path + str(timestamp)
        sign = hmac.new(
            self.partner_key.encode("utf-8"),
            baseStr.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        url = (
            self.host
            + path
            + "?partner_id="
            + str(self.partner_id)
            + "&timestamp="
            + str(timestamp)
            + "&sign="
            + str(sign)
        )
        headers = {"Content-Type": "application/json"}

        response = httpx.post(url, json=body, headers=headers)
        content = json.loads(response.content)
        return content

    def refreshToken(self, refresh_token):
        ts = int(datetime.timestamp(datetime.now()))
        body = {
            "shop_id": self.shop_id,
            "partner_id": self.partner_id,
            "refresh_token": refresh_token,
        }

        path = "/api/v2/auth/access_token/get"
        baseStr = str(self.partner_id) + path + str(ts)
        sign = hmac.new(
            self.partner_key.encode("utf-8"),
            baseStr.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        url = (
            self.host
            + path
            + "?partner_id="
            + str(self.partner_id)
            + "&timestamp="
            + str(ts)
            + "&sign="
            + str(sign)
        )
        headers = {"Content-Type": "application/json"}
        response = httpx.post(url, json=body, headers=headers)

        content = json.loads(response.content)
        return content.get("access_token"), content.get("refresh_token")

    def getOrderList(self, access_token, order_status, time_from, time_to):
        ts = int(datetime.timestamp(datetime.now()))
        path = "/api/v2/order/get_order_list"
        baseStr = (
            str(self.partner_id)
            + path
            + str(ts)
            + access_token
            + str(self.shop_id)
        )
        sign = hmac.new(
            self.partner_key.encode("utf-8"),
            baseStr.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        url = (
            self.host
            + path
            + "?access_token="
            + access_token
            + "&order_status="
            + order_status
            + "&page_size=100&partner_id="
            + str(self.partner_id)
            + "&response_optional_fields=order_status&shop_id="
            + str(self.shop_id)
            + "&sign="
            + str(sign)
            + "&time_from="
            + str(time_from)
            + "&time_range_field=create_time"
            + "&time_to="
            + str(time_to)
            + "&timestamp="
            + str(ts)
        )

        headers = {"Content-Type": "application/json"}
        response = httpx.get(url, headers=headers, follow_redirects=False)
        content = json.loads(response.content)

        return content
