from datetime import UTC, datetime

import pytest

from uk_flood.dag_schedule import should_build_dbt, should_ingest_reference
from uk_flood.ops import evaluate_gate


def _context(hour: int = 2, minute: int = 0) -> dict:
    return {
        "data_interval_end": datetime(2026, 11, 17, hour, minute, tzinfo=UTC),
        "run_id": "manual__2026-11-17",
    }


def test_gate_pass_records_event_without_alert():
    events = []
    alerts = []

    allowed = evaluate_gate(
        "reference",
        _context(2, 0),
        should_ingest_reference,
        recorder=lambda *args: events.append(args),
        alerter=lambda *args: alerts.append(args) or True,
    )

    assert allowed is True
    assert len(events) == 1
    gate, decision, reason, _, fail_mode, alert_sent = events[0]
    assert (gate, decision, reason, fail_mode, alert_sent) == (
        "reference",
        True,
        "scheduled",
        "closed",
        False,
    )
    assert alerts == []


def test_gate_trip_records_event_and_alerts():
    events = []
    alerts = []

    allowed = evaluate_gate(
        "dbt",
        _context(1, 0),
        should_build_dbt,
        recorder=lambda *args: events.append(args),
        alerter=lambda *args: alerts.append(args) or True,
    )

    assert allowed is False
    assert len(events) == 1
    assert events[0][1] is False
    assert events[0][2] == "outside_schedule"
    assert events[0][5] is True
    assert len(alerts) == 1


def test_gate_fail_closed_on_predicate_error():
    events = []

    def boom(_context):
        raise RuntimeError("schedule context missing")

    allowed = evaluate_gate(
        "dbt",
        {},
        boom,
        fail_mode="closed",
        recorder=lambda *args: events.append(args),
        alerter=lambda *args: False,
    )

    assert allowed is False
    assert events[0][1] is False
    assert events[0][2].startswith("gate_error_closed:")


def test_gate_fail_open_on_predicate_error():
    events = []

    def boom(_context):
        raise RuntimeError("schedule context missing")

    allowed = evaluate_gate(
        "reference",
        {},
        boom,
        fail_mode="open",
        recorder=lambda *args: events.append(args),
        alerter=lambda *args: False,
    )

    assert allowed is True
    assert events[0][1] is True
    assert events[0][2].startswith("gate_error_open:")


def test_gate_rejects_unknown_fail_mode():
    with pytest.raises(ValueError):
        evaluate_gate("dbt", {}, should_build_dbt, fail_mode="maybe")


def test_observability_failure_does_not_change_decision():
    def bad_recorder(*_args):
        raise RuntimeError("bigquery unavailable")

    allowed = evaluate_gate(
        "dbt",
        _context(2, 0),
        should_build_dbt,
        recorder=bad_recorder,
        alerter=lambda *args: False,
    )

    assert allowed is True
