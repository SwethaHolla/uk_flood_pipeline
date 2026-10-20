"""End-to-end ingestion against the mock API + duckdb, and CLI smoke tests."""

from __future__ import annotations

import duckdb
from typer.testing import CliRunner

from uk_flood.ea_client import EAClient
from uk_flood.ingest import app
from uk_flood.pipelines import run_ingestion


def test_full_ingestion_loads_all_resources(settings, duckdb_path):
    client = EAClient(settings)
    report = run_ingestion(
        settings=settings, client=client, destination="duckdb", duckdb_path=duckdb_path
    )
    by_resource = {r.resource: r for r in report.resources}
    assert set(by_resource) == {"stations", "measures", "flood_warnings", "readings_latest"}
    assert all(r.status == "success" for r in report.resources)
    assert by_resource["stations"].rows_loaded == 6
    assert by_resource["measures"].rows_loaded == 8
    assert by_resource["flood_warnings"].rows_loaded == 2
    assert by_resource["readings_latest"].rows_loaded == 6

    con = duckdb.connect(duckdb_path)
    try:
        # Quirk 1 evidence: the list-valued reading survived with its flag
        flagged = con.execute(
            "SELECT COUNT(*) FROM uk_flood_raw.readings_latest WHERE value_is_list"
        ).fetchone()[0]
        assert flagged == 1
        # Quirk 6 evidence: naive flood timestamps were promoted to UTC
        null_tz = con.execute(
            "SELECT COUNT(*) FROM uk_flood_raw.flood_warnings WHERE time_raised IS NULL"
        ).fetchone()[0]
        assert null_tz == 0
        # Quirk 2: missing reading values captured, not dropped
        missing = con.execute(
            "SELECT COUNT(*) FROM uk_flood_raw.readings_latest WHERE value_missing"
        ).fetchone()[0]
        assert missing == 1
    finally:
        con.close()


def test_schema_drift_is_tolerated(settings, make_server, duckdb_path):
    """Unexpected new fields must not break the load (contract ignores extras)."""
    from mock_api.app import FaultConfig
    from tests.helpers import with_base_url

    client_settings = with_base_url(settings, make_server(FaultConfig(schema_drift=True)))
    client = EAClient(client_settings)
    report = run_ingestion(
        settings=client_settings,
        client=client,
        destination="duckdb",
        duckdb_path=duckdb_path,
        resources=["stations"],
    )
    assert report.resources[0].status == "success"
    assert report.resources[0].rows_loaded == 6


def test_cli_resources_command():
    runner = CliRunner()
    result = runner.invoke(app, ["resources"])
    assert result.exit_code == 0
    assert "readings_latest" in result.output
    assert "flood_warnings" in result.output


def test_cli_run_against_duckdb_fails_cleanly_without_api(monkeypatch, tmp_path):
    """CLI must surface connection failures as a non-zero exit, not a traceback."""
    monkeypatch.setenv("HTTP_MAX_RETRIES", "1")
    monkeypatch.setenv("HTTP_BACKOFF_BASE", "0.01")
    monkeypatch.setenv("HTTP_TIMEOUT_SECONDS", "2")
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "run",
            "--destination",
            "duckdb",
            "--duckdb-path",
            str(tmp_path / "cli_test.duckdb"),
            "--base-url",
            "http://127.0.0.1:9",  # RFC 5737 TEST-NET address: guaranteed refusal
            "--resources",
            "stations",
        ],
    )
    assert result.exit_code != 0, result.output
