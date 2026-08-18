import pytest
import requests

from common.dummyjson_client import (
    UnexpectedResponseShapeError,
    fetch_carts,
    fetch_products,
)


class FakeResponse:
    def __init__(self, json_data, status_code=200):
        self._json_data = json_data
        self.status_code = status_code

    def json(self):
        return self._json_data

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


def test_fetch_products_returns_full_payload(monkeypatch):
    payload = {"products": [{"id": 1, "title": "Foo"}], "total": 1, "skip": 0, "limit": 0}

    def fake_get(url, params=None, timeout=None):
        assert url.endswith("/products")
        assert params == {"limit": 0}
        return FakeResponse(payload)

    monkeypatch.setattr("common.dummyjson_client.requests.get", fake_get)

    assert fetch_products() == payload


def test_fetch_carts_returns_full_payload(monkeypatch):
    payload = {"carts": [{"id": 1, "products": []}], "total": 1, "skip": 0, "limit": 0}

    def fake_get(url, params=None, timeout=None):
        assert url.endswith("/carts")
        return FakeResponse(payload)

    monkeypatch.setattr("common.dummyjson_client.requests.get", fake_get)

    assert fetch_carts() == payload


def test_fetch_products_raises_on_http_error(monkeypatch):
    monkeypatch.setattr(
        "common.dummyjson_client.requests.get",
        lambda url, params=None, timeout=None: FakeResponse({}, status_code=500),
    )

    with pytest.raises(requests.HTTPError):
        fetch_products()


@pytest.mark.parametrize(
    "payload",
    [
        {"products": []},
        {"not_products": [{"id": 1}]},
        {"products": "not-a-list"},
        ["not", "a", "dict"],
    ],
)
def test_fetch_products_rejects_unexpected_shapes(monkeypatch, payload):
    monkeypatch.setattr(
        "common.dummyjson_client.requests.get",
        lambda url, params=None, timeout=None: FakeResponse(payload),
    )

    with pytest.raises(UnexpectedResponseShapeError):
        fetch_products()
