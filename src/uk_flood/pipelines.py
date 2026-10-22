"""dlt ingestion pipelines: fetch -> validate (contract) -> load -> metadata.

Every contract failure is captured in the ``quarantine`` table with the raw
payload, error reason and load id - the pipeline keeps going. Every run
appends a row per resource to ``run_metadata`` (rows loaded, quarantined,
duration, schema-change flag, load id).
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import dlt
from pydantic import BaseModel, ValidationError

from uk_flood.contracts import (
    FloodWarningContract,
    MeasureContract,
    ReadingContract,
    StationContract,
)
from uk_flood.ea_client import EAClient
from uk_flood.settings import Settings, get_settings

logger = logging.getLogger(__name__)

# Resource name -> (API path, params, contract, dlt table, merge keys)
RESOURCE_SPECS: dict[str, dict[str, Any]] = {
    "stations": {
        "path": "/id/stations",
        "params": None,
        "contract": StationContract,
        "table": "stations",
        "primary_keys": ["id"],
    },
    "measures": {
        "path": "/id/measures",
        "params": None,
        "contract": MeasureContract,
        "table": "measures",
        "primary_keys": ["id"],
    },
    "flood_warnings": {
        "path": "/id/floods",
        "params": None,
        "contract": FloodWarningContract,
        "table": "flood_warnings",
        "primary_keys": ["id"],
    },
    "readings_latest": {
        "path": "/data/readings",
        "params": {"latest": ""},
        "contract": ReadingContract,
        "table": "readings_latest",
        "primary_keys": ["id"],
    },
}

# PRD section 7 quirk 12: readings are idempotent on (measure, dateTime); the
# @id alone is stable but this key is what the history backfill relies on.
READING_HISTORY_SPEC: dict[str, Any] = {
    "contract": ReadingContract,
    "table": "readings",
    "primary_keys": ["measure", "datetime"],
}

# Explicit column types so dlt doesn't drop columns that arrive all-NULL on
# the first run (dlt infers types from data).
RUN_METADATA_COLUMNS: dict[str, dict[str, str]] = {
    "run_id": {"data_type": "text"},
    "load_id": {"data_type": "text"},
    "resource": {"data_type": "text"},
    "rows_loaded": {"data_type": "bigint"},
    "rows_quarantined": {"data_type": "bigint"},
    "duration_s": {"data_type": "double"},
    "started_at": {"data_type": "timestamp"},
    "ended_at": {"data_type": "timestamp"},
    "status": {"data_type": "text"},
    "schema_change_flag": {"data_type": "bool"},
    "schema_hash": {"data_type": "text"},
    "error": {"data_type": "text"},
}

QUARANTINE_COLUMNS: dict[str, dict[str, str]] = {
    "resource": {"data_type": "text"},
    "raw_payload": {"data_type": "text"},
    "error_reason": {"data_type": "text"},
    "error_type": {"data_type": "text"},
    "load_id": {"data_type": "text"},
    "run_id": {"data_type": "text"},
    "failed_at": {"data_type": "timestamp"},
}


@dataclass
class ResourceResult:
    resource: str
    rows_loaded: int = 0
    rows_quarantined: int = 0
    duration_s: float = 0.0
    status: str = "success"
    error: str | None = None
    schema_change: bool = False
    schema_hash: str | None = None


@dataclass
class IngestReport:
    run_id: str
    load_id: str | None = None
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    ended_at: datetime | None = None
    status: str = "success"
    resources: list[ResourceResult] = field(default_factory=list)

    @property
    def total_loaded(self) -> int:
        return sum(r.rows_loaded for r in self.resources)

    @property
    def total_quarantined(self) -> int:
        return sum(r.rows_quarantined for r in self.resources)


def make_pipeline(
    settings: Settings,
    destination: str = "bigquery",
    duckdb_path: str = "data/uk_flood.duckdb",
) -> dlt.Pipeline:
    if destination == "bigquery":
        kwargs: dict[str, Any] = {"location": settings.bq_location}
        if settings.google_application_credentials:
            kwargs["credentials"] = settings.google_application_credentials
        dest = dlt.destinations.bigquery(**kwargs)
    elif destination == "duckdb":
        Path(duckdb_path).parent.mkdir(parents=True, exist_ok=True)
        dest = dlt.destinations.duckdb(duckdb_path)
    else:
        raise ValueError(f"Unsupported destination: {destination}")
    return dlt.pipeline(
        pipeline_name="uk_flood_raw",
        destination=dest,
        dataset_name=settings.dlt_dataset_name,
        dev_mode=False,
    )


def validate_items(
    resource: str,
    items: Iterable[Any],
    contract: type[BaseModel],
    load_id: str,
    limit: int | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split raw items into (valid rows, quarantine rows). Never raises."""
    valid: list[dict[str, Any]] = []
    quarantined: list[dict[str, Any]] = []
    now = datetime.now(UTC)
    for i, item in enumerate(items):
        if limit is not None and i >= limit:
            break
        try:
            model = contract.model_validate(item)
        except ValidationError as exc:
            quarantined.append(
                {
                    "resource": resource,
                    "raw_payload": json.dumps(item, default=str),
                    "error_reason": exc.errors(include_url=False)[0]["msg"][:500],
                    "error_type": "ValidationError",
                    "load_id": load_id,
                    "run_id": None,  # patched by caller
                    "failed_at": now,
                }
            )
            continue
        row = model.model_dump()
        row["_resource"] = resource
        valid.append(row)
    return valid, quarantined


def schema_fingerprint(contract: type[BaseModel]) -> str:
    """Stable hash of the contract's JSON schema - used for drift detection."""
    schema_json = json.dumps(contract.model_json_schema(), sort_keys=True, default=str)
    return hashlib.sha256(schema_json.encode()).hexdigest()[:16]


def _previous_schema_hash(pipeline: dlt.Pipeline, resource: str) -> str | None:
    """Best-effort read of the schema hash recorded on the previous run."""
    try:
        with pipeline.sql_client() as client:
            rows = client.execute_sql(
                "SELECT schema_hash FROM run_metadata "
                "WHERE resource = ? AND schema_hash IS NOT NULL "
                "ORDER BY ended_at DESC LIMIT 1",
                resource,
            )
            for row in rows or []:
                value = row[0] if isinstance(row, (tuple, list)) else row
                if value:
                    return str(value)
    except Exception:
        return None
    return None


def run_ingestion(
    settings: Settings | None = None,
    client: EAClient | None = None,
    destination: str = "bigquery",
    duckdb_path: str = "data/uk_flood.duckdb",
    resources: list[str] | None = None,
    limits: dict[str, int] | None = None,
) -> IngestReport:
    """Run the standard ingestion cycle: fetch, contract-validate, load, log."""
    settings = settings or get_settings()
    owns_client = client is None
    client = client or EAClient(settings)
    run_id = str(uuid4())
    report = IngestReport(run_id=run_id)
    pipeline = make_pipeline(settings, destination=destination, duckdb_path=duckdb_path)

    selected = resources or list(RESOURCE_SPECS)
    limits = limits or {}
    quarantine_rows: list[dict[str, Any]] = []
    load_resources: list[Any] = []

    try:
        for name in selected:
            spec = RESOURCE_SPECS[name]
            started = datetime.now(UTC)
            result = ResourceResult(resource=name)
            try:
                items = client.get_items(spec["path"], spec["params"])
                valid, quarantined = validate_items(
                    name, items, spec["contract"], load_id=run_id, limit=limits.get(name)
                )
                for q in quarantined:
                    q["run_id"] = run_id
                quarantine_rows.extend(quarantined)
                if valid:
                    load_resources.append(
                        dlt.resource(
                            valid,
                            name=spec["table"],
                            write_disposition="merge",
                            primary_key=spec["primary_keys"],
                        )
                    )
                    if name == "readings_latest":
                        # Dual-write: the rolling ?latest snapshot also feeds the
                        # `readings` history table (merge on measure+datetime),
                        # so fct_readings accumulates without the seed script.
                        load_resources.append(
                            dlt.resource(
                                valid,
                                name="readings",
                                write_disposition="merge",
                                primary_key=READING_HISTORY_SPEC["primary_keys"],
                            )
                        )
                result.rows_loaded = len(valid)
                result.rows_quarantined = len(quarantined)
                result.schema_hash = schema_fingerprint(spec["contract"])
                result.schema_change = _previous_schema_hash(pipeline, name) not in (
                    None,
                    result.schema_hash,
                )
            except Exception as exc:
                logger.exception("Resource %s failed", name)
                result.status = "failed"
                result.error = str(exc)[:500]
                report.status = "partial" if report.status == "success" else report.status
            result.duration_s = (datetime.now(UTC) - started).total_seconds()
            report.resources.append(result)

        if report.resources and all(r.status == "failed" for r in report.resources):
            report.status = "failed"

        if quarantine_rows:
            load_resources.append(
                dlt.resource(
                    quarantine_rows,
                    name="quarantine",
                    write_disposition="append",
                    columns=QUARANTINE_COLUMNS,
                )
            )

        if load_resources:
            load_info = pipeline.run(load_resources)
            report.load_id = _extract_load_id(load_info)
        else:
            logger.warning("Nothing to load for run %s", run_id)

        report.ended_at = datetime.now(UTC)
        _write_run_metadata(pipeline, report)
    finally:
        if owns_client:
            client.close()

    return report


def load_readings_history(
    readings: Iterable[dict[str, Any]],
    settings: Settings | None = None,
    destination: str = "bigquery",
    duckdb_path: str = "data/uk_flood.duckdb",
) -> ResourceResult:
    """Load historical readings rows (already raw dicts) into ``readings``."""
    settings = settings or get_settings()
    run_id = str(uuid4())
    contract = READING_HISTORY_SPEC["contract"]
    valid, quarantined = validate_items("readings", readings, contract, load_id=run_id)
    for q in quarantined:
        q["run_id"] = run_id

    pipeline = make_pipeline(settings, destination=destination, duckdb_path=duckdb_path)
    load_resources: list[Any] = [
        dlt.resource(
            valid,
            name="readings",
            write_disposition="merge",
            primary_key=READING_HISTORY_SPEC["primary_keys"],
        )
    ]
    if quarantined:
        load_resources.append(
            dlt.resource(
                quarantined,
                name="quarantine",
                write_disposition="append",
                columns=QUARANTINE_COLUMNS,
            )
        )

    report = IngestReport(run_id=run_id)
    result = ResourceResult(
        resource="readings", rows_loaded=len(valid), rows_quarantined=len(quarantined)
    )
    try:
        if valid:
            load_info = pipeline.run(load_resources)
            report.load_id = _extract_load_id(load_info)
            report.resources.append(result)
            report.ended_at = datetime.now(UTC)
            _write_run_metadata(pipeline, report)
        else:
            result.status = "skipped"
            report.resources.append(result)
    except Exception as exc:
        result.status = "failed"
        result.error = str(exc)[:500]
        report.resources.append(result)
    return result


def _extract_load_id(load_info: Any) -> str | None:
    try:
        return load_info.load_id  # type: ignore[no-any-return]
    except Exception:
        try:
            return load_info["load_id"]  # type: ignore[index]
        except Exception:
            return None


def _write_run_metadata(pipeline: dlt.Pipeline, report: IngestReport) -> None:
    """Append one run_metadata row per resource (+ a summary row).

    A failure to persist run metadata is a *silent* loss of the pipeline's whole
    audit trail (this is exactly how BigQuery ended up with no ``run_metadata``
    table). We therefore re-raise so the caller/caller-DAG sees the failure and
    marks the run as failed, instead of logging-and-forgetting.
    """
    ended = report.ended_at or datetime.now(UTC)
    total_duration = (ended - report.started_at).total_seconds()
    rows: list[dict[str, Any]] = []
    for r in report.resources:
        rows.append(
            {
                "run_id": report.run_id,
                "load_id": report.load_id,
                "resource": r.resource,
                "rows_loaded": r.rows_loaded,
                "rows_quarantined": r.rows_quarantined,
                "duration_s": r.duration_s,
                "started_at": report.started_at,
                "ended_at": ended,
                "status": r.status,
                "schema_change_flag": r.schema_change,
                "schema_hash": r.schema_hash,
                "error": r.error,
            }
        )
    rows.append(
        {
            "run_id": report.run_id,
            "load_id": report.load_id,
            "resource": "__summary__",
            "rows_loaded": report.total_loaded,
            "rows_quarantined": report.total_quarantined,
            "duration_s": total_duration,
            "started_at": report.started_at,
            "ended_at": ended,
            "status": report.status,
            "schema_change_flag": any(r.schema_change for r in report.resources),
            "schema_hash": None,
            "error": None,
        }
    )
    try:
        pipeline.run(
            dlt.resource(
                rows, name="run_metadata", write_disposition="append", columns=RUN_METADATA_COLUMNS
            )
        )
    except Exception as exc:
        logger.exception("Failed to write run_metadata for run %s", report.run_id)
        report.status = "failed"
        raise RuntimeError(
            f"Failed to persist run_metadata for run {report.run_id}: {exc}"
        ) from exc
