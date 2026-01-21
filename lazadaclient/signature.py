# signature.py
import hashlib
import hmac


def generate_signature(app_secret: str, api_path: str, parameters: dict) -> str:
    """
    Tạo chữ ký HMAC-SHA256 cho Lazada API.

    Quy trình:
    1. Sort parameters theo key (alphabetically)
    2. Nối: api_path + key1 + value1 + key2 + value2 + ...
    3. HMAC-SHA256 với app_secret làm key
    4. Chuyển thành uppercase hex
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
