from __future__ import annotations

import socket
import threading
import time
from pathlib import Path

import pytest
import uvicorn

from mock_api.app import FaultConfig, create_app
from uk_flood.settings import Settings


class MockServer:
    """Real HTTP server around the mock EA API (sync httpx client needs one)."""

    def __init__(self, faults: FaultConfig | None = None) -> None:
        self.app = create_app(faults)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("127.0.0.1", 0))
        self.port = sock.getsockname()[1]
        sock.close()
        self.config = uvicorn.Config(self.app, host="127.0.0.1", port=self.port, log_level="error")
        self.server = uvicorn.Server(self.config)
        self.thread: threading.Thread | None = None

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def start(self) -> str:
        self.thread = threading.Thread(target=self.server.run, daemon=True)
        self.thread.start()
        deadline = time.time() + 10
        while not self.server.started:
            if time.time() > deadline:
                raise RuntimeError("mock server failed to start")
            time.sleep(0.01)
        return self.url

    def stop(self) -> None:
        self.server.should_exit = True
        if self.thread:
            self.thread.join(timeout=5)


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
