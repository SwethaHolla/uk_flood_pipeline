"""Operational gate events and optional alerts.

The Airflow ShortCircuit gates are schedule predicates, not BigQuery cost
queries: they read ``data_interval_end`` (falling back to ``logical_date``) and
therefore cost **zero query bytes**. A trip is still observable: it appends one
row to ``<OPS_DATASET>.<OPS_GATE_TABLE>`` via a BigQuery streaming insert (free),
and can optionally POST to a webhook. Alerting is off unless
``OPS_ALERT_WEBHOOK_URL`` is set.

Observability must never change the cost decision. If the ops insert or alert
fails we log it and still return the gate decision.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import httpx

logger = logging.getLogger(__name__)

OPS_DATASET = os.environ.get("OPS_DATASET", "uk_flood_ops")
OPS_GATE_TABLE = os.environ.get("OPS_GATE_TABLE", "gate_events")
OPS_GATE_EVENTS_ENABLED = os.environ.get("OPS_GATE_EVENTS_ENABLED", "1").lower() not in {
    "0",
    "false",
    "no",
}
OPS_ALERT_WEBHOOK_URL = os.environ.get("OPS_ALERT_WEBHOOK_URL", "")

GateRecorder = Callable[[str, bool, str, dict[str, Any], str, bool], None]
GateAlerter = Callable[[str, str, dict[str, Any]], bool]


def _interval_end(context: dict[str, Any]) -> datetime | None:
    return context.get("data_interval_end") or context.get("logical_date")


def _gate_row(
    gate: str,
    allowed: bool,
    reason: str,
    context: dict[str, Any],
    fail_mode: str,
    alert_sent: bool,
) -> dict[str, Any]:
    end = _interval_end(context)
    dag = context.get("dag")
    task = context.get("task")
    return {
        "event_time": datetime.now(UTC).isoformat(),
        "dag_id": getattr(dag, "dag_id", None),
        "task_id": getattr(task, "task_id", None),
        "run_id": context.get("run_id"),
        "gate": gate,
        "decision": allowed,
        "reason": reason,
        "interval_end": end.isoformat() if end is not None else None,
        "fail_mode": fail_mode,
        "alert_sent": alert_sent,
    }


def _ensure_gate_table(client: Any, project: str, location: str) -> Any:
    from google.cloud import bigquery

    dataset_ref = bigquery.Dataset(f"{project}.{OPS_DATASET}")
    dataset_ref.location = location
    client.create_dataset(dataset_ref, exists_ok=True)
    table_ref = dataset_ref.table(OPS_GATE_TABLE)
    schema = [
        bigquery.SchemaField("event_time", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("dag_id", "STRING"),
        bigquery.SchemaField("task_id", "STRING"),
        bigquery.SchemaField("run_id", "STRING"),
        bigquery.SchemaField("gate", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("decision", "BOOLEAN", mode="REQUIRED"),
        bigquery.SchemaField("reason", "STRING"),
        bigquery.SchemaField("interval_end", "TIMESTAMP"),
        bigquery.SchemaField("fail_mode", "STRING"),
        bigquery.SchemaField("alert_sent", "BOOLEAN"),
    ]
    try:
        client.get_table(table_ref)
    except Exception:
        client.create_table(bigquery.Table(table_ref, schema=schema), exists_ok=True)
    return table_ref


def record_gate_event(
    gate: str,
    allowed: bool,
    reason: str,
    context: dict[str, Any],
    fail_mode: str,
    alert_sent: bool,
) -> None:
    """Append one gate decision row. Best-effort: never raises."""
    if not OPS_GATE_EVENTS_ENABLED:
        return
    project = os.environ.get("GCP_PROJECT_ID", "")
    if not project:
        logger.warning("GCP_PROJECT_ID is not set; skipping gate event for %s", gate)
        return
    try:
        from google.cloud import bigquery

        client = bigquery.Client(project=project)
        table_ref = _ensure_gate_table(
            client, project, os.environ.get("BQ_LOCATION", "europe-west2")
        )
        errors = client.insert_rows_json(
            table_ref,
            [_gate_row(gate, allowed, reason, context, fail_mode, alert_sent)],
        )
        if errors:
            logger.error("Gate event insert failed for %s: %s", gate, errors)
    except Exception:
        logger.exception("Failed to record gate event for %s", gate)


def send_gate_alert(gate: str, reason: str, context: dict[str, Any]) -> bool:
    """POST a gate trip to ``OPS_ALERT_WEBHOOK_URL``. Off by default."""
    if not OPS_ALERT_WEBHOOK_URL:
        return False
    payload = {
        "gate": gate,
        "reason": reason,
        "interval_end": (end.isoformat() if (end := _interval_end(context)) is not None else None),
        "run_id": context.get("run_id"),
    }
    try:
        response = httpx.post(OPS_ALERT_WEBHOOK_URL, json=payload, timeout=10)
        response.raise_for_status()
        return True
    except Exception:
        logger.exception("Failed to send gate alert for %s", gate)
        return False


def evaluate_gate(
    gate: str,
    context: dict[str, Any],
    predicate: Callable[[dict[str, Any]], bool],
    fail_mode: str = "closed",
    recorder: GateRecorder | None = None,
    alerter: GateAlerter | None = None,
) -> bool:
    """Evaluate a schedule gate, record the decision, and alert on trips.

    ``fail_mode`` only applies if the predicate raises. ``closed`` skips the
    expensive task (protects the quota); ``open`` runs it (protects freshness).
    A missing interval is a normal predicate result (False), not an exception.
    """
    if fail_mode not in {"closed", "open"}:
        raise ValueError(f"Unsupported fail_mode: {fail_mode}")
    record = recorder or record_gate_event
    alert = alerter or send_gate_alert
    try:
        allowed = bool(predicate(context))
        reason = "scheduled" if allowed else "outside_schedule"
    except Exception as exc:
        allowed = fail_mode == "open"
        reason = f"gate_error_{fail_mode}:{exc}"[:500]

    alert_sent = False
    if not allowed:
        try:
            alert_sent = bool(alert(gate, reason, context))
        except Exception:
            logger.exception("Gate alerter failed for %s", gate)

    try:
        record(gate, allowed, reason, context, fail_mode, alert_sent)
    except Exception:
        logger.exception("Gate recorder failed for %s", gate)
    return allowed
