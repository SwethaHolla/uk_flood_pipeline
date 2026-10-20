"""Idempotency: re-running the same ingestion window adds 0 new duplicate rows.

PRD success criterion 2. Runs the full pipeline twice against a local duckdb
destination through the mock API and asserts stable row counts and unique
natural keys in every raw table.
"""

from __future__ import annotations

import duckdb

from uk_flood.ea_client import EAClient
from uk_flood.pipelines import run_ingestion

TABLES = ["stations", "measures", "flood_warnings", "readings_latest"]


def snapshot(con: duckdb.DuckDBPyConnection) -> dict[str, tuple[int, int]]:
    """table -> (row count, distinct natural key count)."""
    out = {}
    for table in TABLES:
        count = con.execute(f"SELECT COUNT(*) FROM uk_flood_raw.{table}").fetchone()[0]
        distinct = con.execute(f"SELECT COUNT(DISTINCT id) FROM uk_flood_raw.{table}").fetchone()[0]
        out[table] = (count, distinct)
    return out


def test_second_run_creates_zero_duplicates(settings, duckdb_path):
    client = EAClient(settings)
    first = run_ingestion(
        settings=settings, client=client, destination="duckdb", duckdb_path=duckdb_path
    )
    assert first.status in ("success", "partial")
    assert first.total_quarantined == 0
    assert first.total_loaded == 6 + 8 + 2 + 6  # fixtures: stations/measures/floods/readings

    con = duckdb.connect(duckdb_path)
    try:
        before = snapshot(con)
    finally:
        con.close()

    # Idempotency: same window, second run
    client = EAClient(settings)
    second = run_ingestion(
        settings=settings, client=client, destination="duckdb", duckdb_path=duckdb_path
    )
    assert second.total_loaded == first.total_loaded

    con = duckdb.connect(duckdb_path)
    try:
        after = snapshot(con)
    finally:
        con.close()

    assert before == after, f"row counts changed on re-run: {before} -> {after}"
    for table, (count, distinct) in after.items():
        assert count == distinct, f"{table} contains duplicate natural keys"


def test_run_metadata_records_every_run(settings, duckdb_path):
    client = EAClient(settings)
    run_ingestion(settings=settings, client=client, destination="duckdb", duckdb_path=duckdb_path)

    con = duckdb.connect(duckdb_path)
    try:
        rows = con.execute(
            "SELECT resource, rows_loaded, rows_quarantined, status, run_id, load_id "
            "FROM uk_flood_raw.run_metadata ORDER BY resource"
        ).fetchall()
    finally:
        con.close()

    resources = {r[0] for r in rows}
    assert "stations" in resources
    assert "__summary__" in resources
    assert len(rows) == 5  # 4 resources + summary
    for row in rows:
        assert row[4], "run_id missing"
