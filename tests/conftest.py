from __future__ import annotations

from pathlib import Path

import pytest

from mock_api.app import FaultConfig
from mock_api.server import MockServer
from uk_flood.settings import Settings


@pytest.fixture
def make_server():
    servers: list[MockServer] = []

    def _make(faults: FaultConfig | None = None) -> str:
        server = MockServer(faults)
        url = server.start()
        servers.append(server)
        return url

    yield _make
    for server in servers:
        server.stop()


@pytest.fixture
def mock_server_url(make_server) -> str:
    return make_server()


@pytest.fixture
def settings(mock_server_url) -> Settings:
    return Settings(
        ea_api_base_url=mock_server_url,
        http_timeout_seconds=10.0,
        http_max_retries=3,
        http_backoff_base=0.01,
    )


@pytest.fixture
def duckdb_path(tmp_path: Path) -> str:
    return str(tmp_path / "uk_flood.duckdb")
