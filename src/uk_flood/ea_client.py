"""Resilient HTTP client for the EA Flood Monitoring API.

Handles the documented API behaviour (PRD §7 quirks 10 & 13):

- follows redirects (the beta service may redirect under load)
- retries with exponential backoff + jitter on timeouts, 429 and 5xx
- honours ``Retry-After`` on 429 responses
- raises typed errors once retries are exhausted
"""

from __future__ import annotations

import random
import time
from typing import Any

import httpx

from uk_flood.settings import Settings, get_settings


class EARequestError(RuntimeError):
    """Raised when a request ultimately fails after retries."""


class EAClient:
    def __init__(
        self,
        settings: Settings | None = None,
        transport: httpx.BaseTransport | None = None,
        client: httpx.Client | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self._client = client or httpx.Client(
            base_url=self.settings.ea_api_base_url,
            timeout=self.settings.http_timeout_seconds,
            follow_redirects=True,
            transport=transport,
            headers={
                "User-Agent": "uk-flood-pipeline/0.1 (+https://github.com/SwethaHolla/uk_flood_pipeline)"
            },
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> EAClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def get_items(self, path: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """GET ``path`` and return the ``items`` array from the standard envelope."""
        payload = self._get_json(path, params)
        items = payload.get("items", [])
        if not isinstance(items, list):
            raise EARequestError(
                f"Expected 'items' to be a list at {path}, got {type(items).__name__}"
            )
        return items

    def _get_json(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        max_retries = self.settings.http_max_retries
        last_error: Exception | None = None

        for attempt in range(max_retries + 1):
            try:
                response = self._client.get(path, params=params)
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                last_error = exc
                if attempt >= max_retries:
                    break
                self._sleep(attempt)
                continue

            if response.status_code == 429 or response.status_code >= 500:
                last_error = EARequestError(f"HTTP {response.status_code} from {path}")
                if attempt >= max_retries:
                    break
                retry_after = response.headers.get("Retry-After")
                self._sleep(attempt, retry_after=float(retry_after) if retry_after else None)
                continue

            if response.status_code >= 400:
                # Client errors (400/404/...) are deterministic — do not retry.
                raise EARequestError(
                    f"HTTP {response.status_code} from {path}: {response.text[:300]}"
                )

            return response.json()

        raise EARequestError(f"Request to {path} failed after {max_retries} retries: {last_error}")

    def _sleep(self, attempt: int, retry_after: float | None = None) -> None:
        if retry_after is not None:
            time.sleep(min(retry_after, 60.0))
            return
        base = self.settings.http_backoff_base
        delay = base * (2**attempt) + random.uniform(0, base)
        time.sleep(min(delay, 60.0))
