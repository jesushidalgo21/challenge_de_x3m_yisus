import requests

BASE_URL = "https://dummyjson.com"
REQUEST_TIMEOUT_SECONDS = 30


class UnexpectedResponseShapeError(Exception):
    pass


def _validate_shape(payload, list_key):
    if not isinstance(payload, dict):
        raise UnexpectedResponseShapeError(f"Expected a JSON object, got {type(payload)}")
    if list_key not in payload:
        raise UnexpectedResponseShapeError(f"Missing '{list_key}' key in response")
    if not isinstance(payload[list_key], list):
        raise UnexpectedResponseShapeError(f"'{list_key}' is not a list")
    if len(payload[list_key]) == 0:
        raise UnexpectedResponseShapeError(f"'{list_key}' is empty")
    return payload


def fetch_products():
    response = requests.get(
        f"{BASE_URL}/products", params={"limit": 0}, timeout=REQUEST_TIMEOUT_SECONDS
    )
    response.raise_for_status()
    return _validate_shape(response.json(), "products")


def fetch_carts():
    response = requests.get(
        f"{BASE_URL}/carts", params={"limit": 0}, timeout=REQUEST_TIMEOUT_SECONDS
    )
    response.raise_for_status()
    return _validate_shape(response.json(), "carts")
