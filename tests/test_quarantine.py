"""Quarantine behaviour: contract failures are captured, never lost, never fatal."""

from __future__ import annotations

import json

import duckdb

from mock_api.app import FaultConfig
from uk_flood.contracts import StationContract
from uk_flood.ea_client import EAClient
from uk_flood.pipelines import run_ingestion, validate_items


def test_validate_items_splits_valid_and_quarantine():
    items = [
        {"@id": ".../stations/good", "lat": 51.0},
        {"@id": ".../stations/bad", "lat": "not-a-number"},
        {"label": "no id at all"},
    ]
    valid, quarantined = validate_items("stations", items, StationContract, load_id="run-1")
    assert len(valid) == 1
    assert len(quarantined) == 2
    q = quarantined[0]
    assert q["resource"] == "stations"
    assert q["load_id"] == "run-1"
    assert q["error_reason"]
    payload = json.loads(q["raw_payload"])
    assert payload["@id"] == ".../stations/bad"
    assert q["failed_at"] is not None


def test_quarantine_never_raises_on_garbage():
    garbage = [None, 42, "a string", [], {"@id": None}]
    valid, quarantined = validate_items("stations", garbage, StationContract, load_id="run-2")
    assert valid == []
    assert len(quarantined) == len(garbage)


def test_pipeline_continues_and_quarantines_bad_row(settings, duckdb_path, make_server):
    """PRD success criterion 4: inject a bad row; pipeline completes; row lands
    in quarantine with payload + reason + load id."""
    from tests.helpers import with_base_url

    url = make_server(FaultConfig(inject_bad_item=True))
    client_settings = with_base_url(settings, url)
    client = EAClient(client_settings)
    report = run_ingestion(
        settings=client_settings, client=client, destination="duckdb", duckdb_path=duckdb_path
    )
    assert report.status in ("success", "partial")
    stations_result = next(r for r in report.resources if r.resource == "stations")
    assert stations_result.rows_loaded == 6
    assert stations_result.rows_quarantined == 1

    con = duckdb.connect(duckdb_path)
    try:
        rows = con.execute(
            "SELECT resource, raw_payload, error_reason, load_id FROM uk_flood_raw.quarantine"
        ).fetchall()
    finally:
        con.close()
    assert len(rows) == 1
    resource, raw_payload, error_reason, load_id = rows[0]
    assert resource == "stations"
    assert "not-a-number" in raw_payload
    assert error_reason
    assert load_id  # ties the quarantined row to the ingestion run
