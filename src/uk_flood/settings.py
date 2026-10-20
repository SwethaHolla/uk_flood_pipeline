"""Runtime configuration, sourced from environment variables (see .env.example)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "")
    return int(raw) if raw else default


def load_dotenv(path: str | Path | None = None) -> None:
    """Tiny .env loader (no external dependency); existing env vars win."""
    env_path = Path(path) if path else Path(".env")
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key, value = key.strip(), value.strip().strip("'\"")
        os.environ.setdefault(key, value)


@dataclass(frozen=True)
class Settings:
    # GCP / BigQuery
    gcp_project_id: str = field(default_factory=lambda: _env("GCP_PROJECT_ID", "ukfloodpipeline"))
    bq_location: str = field(default_factory=lambda: _env("BQ_LOCATION", "US"))
    bq_raw_dataset: str = field(default_factory=lambda: _env("BQ_RAW_DATASET", "uk_flood_raw"))
    max_bytes_billed: int = field(
        default_factory=lambda: _env_int("MAX_BYTES_BILLED", 10 * 1024**3)
    )
    google_application_credentials: str = field(
        default_factory=lambda: _env("GOOGLE_APPLICATION_CREDENTIALS", "")
    )

    # EA API
    ea_api_base_url: str = field(
        default_factory=lambda: _env(
            "EA_API_BASE_URL", "https://environment.data.gov.uk/flood-monitoring"
        )
    )
    http_timeout_seconds: float = field(
        default_factory=lambda: float(_env("HTTP_TIMEOUT_SECONDS", "30"))
    )
    http_max_retries: int = field(default_factory=lambda: _env_int("HTTP_MAX_RETRIES", 5))
    http_backoff_base: float = field(
        default_factory=lambda: float(_env("HTTP_BACKOFF_BASE", "1.0"))
    )

    # dlt
    dlt_dataset_name: str = field(default_factory=lambda: _env("DLT_DATASET_NAME", "uk_flood_raw"))

    @property
    def raw_table_prefix(self) -> str:
        return self.bq_raw_dataset


def get_settings() -> Settings:
    """Return settings, auto-loading a local `.env` if present."""
    load_dotenv()
    return Settings()
