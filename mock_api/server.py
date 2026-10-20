"""Run the mock EA API as a real HTTP server (used by scripts and tests)."""

from __future__ import annotations

import socket
import threading
import time

import uvicorn

from mock_api.app import FaultConfig, create_app


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
