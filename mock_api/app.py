"""Local mock of the EA Flood Monitoring API.

Serves canned fixture payloads (including the documented quirks) and supports
deterministic fault injection for the chaos harness:

- ``fail_all``: every request returns ``status_code`` (default 503)
- ``n_failures``: fail the next N requests, then recover (for retry tests)
- ``delay_seconds``: sleep before responding (timeout tests)
- ``malformed_json``: return a truncated/invalid JSON body
- ``schema_drift``: inject unexpected fields into items
- ``empty_items``: return an empty ``items`` array
- ``inject_bad_item``: append a contract-breaking station (for quarantine tests)
"""

from __future__ import annotations

import asyncio
import copy
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query, Request, Response

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@dataclass
class FaultConfig:
    fail_all: bool = False
    status_code: int = 503
    n_failures: int = 0
    delay_seconds: float = 0.0
    malformed_json: bool = False
    schema_drift: bool = False
    empty_items: bool = False
    inject_bad_item: bool = False
    _failures_remaining: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        self._failures_remaining = self.n_failures

    def should_fail(self) -> bool:
        if self.fail_all:
            return True
        if self._failures_remaining > 0:
            self._failures_remaining -= 1
            return True
        return False


def _load_fixture(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES_DIR / f"{name}.json").read_text(encoding="utf-8"))


def _envelope(items: list[dict[str, Any]]) -> dict[str, Any]:
    payload = _load_fixture("stations")  # any fixture has a valid meta block
    return {"@context": payload["@context"], "meta": payload["meta"], "items": items}


def create_app(faults: FaultConfig | None = None) -> FastAPI:
    faults = faults or FaultConfig()
    app = FastAPI(title="EA Flood Monitoring API (mock)", version="0.9")
    app.state.faults = faults

    @app.middleware("http")
    async def inject_faults(request: Request, call_next):
        if faults.delay_seconds:
            await asyncio.sleep(faults.delay_seconds)
        if faults.should_fail():
            return Response(
                content=json.dumps({"error": "mock failure", "status": faults.status_code}),
                status_code=faults.status_code,
                media_type="application/json",
                headers={"Retry-After": "0"} if faults.status_code == 429 else None,
            )
        return await call_next(request)

    def _items(name: str, request: Request) -> list[dict[str, Any]]:
        if faults.empty_items:
            return []
        items = copy.deepcopy(_load_fixture(name)["items"])
        if faults.schema_drift:
            for item in items:
                item["unexpectedNewField"] = {"nested": True}
                item.pop("label", None)
        if faults.inject_bad_item and name == "stations":
            # lat cannot be coerced to float -> contract ValidationError
            items.append(
                {
                    "@id": "http://environment.data.gov.uk/flood-monitoring/id/stations/bad-1",
                    "label": "Contract-breaking station",
                    "lat": "not-a-number",
                }
            )
        return items

    @app.get("/id/stations")
    def stations(request: Request) -> dict[str, Any]:
        return _envelope(_items("stations", request))

    @app.get("/id/measures")
    def measures(request: Request) -> dict[str, Any]:
        return _envelope(_items("measures", request))

    @app.get("/id/floods")
    def floods(request: Request) -> dict[str, Any]:
        return _envelope(_items("floods", request))

    @app.get("/data/readings")
    def readings(
        request: Request,
        latest: str | None = Query(default=None),
        date: str | None = Query(default=None),
        measure: str | None = Query(default=None),
    ) -> dict[str, Any]:
        items = _items("readings", request)
        if latest is None and date is not None:
            # Fixture readings all sit on 2026-10-20; other dates -> empty
            items = [i for i in items if str(i.get("dateTime", "")).startswith(date)]
        if measure:
            items = [i for i in items if i.get("measure") == measure]
        return _envelope(items)

    return app


app = create_app()
