"""Client resilience tests: retries, backoff, 429/5xx handling, no retry on 4xx."""

from __future__ import annotations

import httpx
import pytest

from uk_flood.ea_client import EAClient, EARequestError
from uk_flood.settings import Settings


def make_settings(**overrides) -> Settings:
    defaults = dict(
        ea_api_base_url="http://mock",
        http_timeout_seconds=2.0,
        http_max_retries=3,
        http_backoff_base=0.001,
    )
    defaults.update(overrides)
    return Settings(**defaults)


def test_retries_on_5xx_then_succeeds():
    attempts = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        attempts["n"] += 1
        if attempts["n"] < 3:
            return httpx.Response(503)
        return httpx.Response(200, json={"items": [{"@id": "x"}]})

    with EAClient(make_settings(), transport=httpx.MockTransport(handler)) as client:
        items = client.get_items("/id/stations")
    assert attempts["n"] == 3
    assert items == [{"@id": "x"}]


def test_respects_retry_after_header_on_429():
    attempts = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        attempts["n"] += 1
        if attempts["n"] == 1:
            return httpx.Response(429, headers={"Retry-After": "0"})
        return httpx.Response(200, json={"items": []})

    with EAClient(make_settings(), transport=httpx.MockTransport(handler)) as client:
        assert client.get_items("/id/floods") == []
    assert attempts["n"] == 2


def test_exhausted_retries_raise_typed_error():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    with (
        EAClient(make_settings(), transport=httpx.MockTransport(handler)) as client,
        pytest.raises(EARequestError, match="failed after 3 retries"),
    ):
        client.get_items("/id/stations")


def test_400_is_not_retried():
    attempts = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        attempts["n"] += 1
        return httpx.Response(400, text="bad query param")

    with (
        EAClient(make_settings(), transport=httpx.MockTransport(handler)) as client,
        pytest.raises(EARequestError, match="HTTP 400"),
    ):
        client.get_items("/data/readings", {"since": "2026-01-01"})
    assert attempts["n"] == 1, "deterministic client errors must not be retried"


def test_timeouts_are_retried():
    attempts = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise httpx.ConnectTimeout("boom")
        return httpx.Response(200, json={"items": [{"@id": "y"}]})

    with EAClient(make_settings(), transport=httpx.MockTransport(handler)) as client:
        items = client.get_items("/id/measures")
    assert attempts["n"] == 2
    assert items == [{"@id": "y"}]


def test_redirects_are_followed():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "mock" and request.url.path == "/id/stations":
            return httpx.Response(307, headers={"Location": "http://mock/id/stations-v2"})
        return httpx.Response(200, json={"items": [{"@id": "redirected"}]})

    with EAClient(make_settings(), transport=httpx.MockTransport(handler)) as client:
        items = client.get_items("/id/stations")
    assert items == [{"@id": "redirected"}]


def test_non_list_items_raises():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"items": {"oops": True}})

    with (
        EAClient(make_settings(), transport=httpx.MockTransport(handler)) as client,
        pytest.raises(EARequestError, match="Expected 'items' to be a list"),
    ):
        client.get_items("/id/stations")
