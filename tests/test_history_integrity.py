"""History integrity tests (Phase B item 2).

Guards against the failure mode found in the audit: ``readings`` contained only
the latest snapshot (one row per measure) and never any backfilled history.

These run fully offline against DuckDB and the local mock API, so they are safe
for CI. They assert on *schema shape and properties* (span, per-day floor,
duplicate-freedom) rather than exact cloud row counts.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import duckdb

from uk_flood.ea_client import EAClient
from uk_flood.pipelines import load_readings_history, run_ingestion


def _make_readings(n_days: int, per_day: int = 3) -> list[dict]:
    """Synthetic historical readings spanning ``n_days`` for one measure."""
    base = datetime(2026, 9, 1, 0, 0, tzinfo=UTC)
    rows: list[dict] = []
    for day in range(n_days):
        for k in range(per_day):
            ts = base + timedelta(days=day, hours=k * 6)
            rows.append(
                {
                    "@id": f"reading-{day}-{k}",
                    "measure": "http://example/measure/1",
                    "dateTime": ts.isoformat(),
                    "value": 1.0 + day + k,
                }
            )
    return rows


def test_load_history_spans_requested_window(settings, duckdb_path):
    n_days = 7
    load_readings_history(
        _make_readings(n_days),
        settings=settings,
        destination="duckdb",
        duckdb_path=duckdb_path,
    )
    con = duckdb.connect(duckdb_path)
    try:
        span = con.execute(
            "SELECT DATE_DIFF('day', MIN(datetime), MAX(datetime)) "
            "FROM uk_flood_raw.readings"
        ).fetchone()[0]
        count = con.execute("SELECT COUNT(*) FROM uk_flood_raw.readings").fetchone()[0]
    finally:
        con.close()
    assert count == n_days * 3
    assert span == n_days - 1, f"history spans {span} day-gaps, expected {n_days - 1}"


def test_min_rows_per_day(settings, duckdb_path):
    n_days = 7
    load_readings_history(
        _make_readings(n_days, per_day=3),
        settings=settings,
        destination="duckdb",
        duckdb_path=duckdb_path,
    )
    con = duckdb.connect(duckdb_path)
    try:
        days = con.execute(
            "SELECT CAST(datetime AS DATE) d, COUNT(*) c "
            "FROM uk_flood_raw.readings GROUP BY d ORDER BY d"
        ).fetchall()
    finally:
        con.close()
    assert len(days) == n_days, f"expected {n_days} distinct days, got {len(days)}"
    for d, c in days:
        assert c >= 1, f"day {d} has {c} rows (silent partial load)"


def test_history_rerun_creates_zero_duplicates(settings, duckdb_path):
    rows = _make_readings(7)
    load_readings_history(rows, settings=settings, destination="duckdb", duckdb_path=duckdb_path)
    first = _snapshot(duckdb_path)
    # Re-load the identical window: merge on (measure, datetime) must be a no-op.
    load_readings_history(rows, settings=settings, destination="duckdb", duckdb_path=duckdb_path)
    second = _snapshot(duckdb_path)
    assert first == second, f"re-run changed readings: {first} -> {second}"
    count, distinct = second
    assert count == distinct, "readings contains duplicate (measure, datetime) keys"


def _snapshot(duckdb_path: str) -> tuple[int, int]:
    con = duckdb.connect(duckdb_path)
    try:
        return (
            con.execute("SELECT COUNT(*) FROM uk_flood_raw.readings").fetchone()[0],
            con.execute(
                "SELECT COUNT(*) FROM ("
                "  SELECT DISTINCT measure, datetime FROM uk_flood_raw.readings)"
            ).fetchone()[0],
        )
    finally:
        con.close()


def test_dual_write_feeds_readings_from_latest(settings, duckdb_path):
    """The rolling ?latest snapshot must also populate the readings history table."""
    client = EAClient(settings)
    run_ingestion(settings=settings, client=client, destination="duckdb", duckdb_path=duckdb_path)
    con = duckdb.connect(duckdb_path)
    try:
        latest = con.execute("SELECT COUNT(*) FROM uk_flood_raw.readings_latest").fetchone()[0]
        history = con.execute("SELECT COUNT(*) FROM uk_flood_raw.readings").fetchone()[0]
    finally:
        con.close()
    assert latest > 0
    assert history == latest, (
        "readings (history) must be populated by the readings_latest dual-write; "
        f"got latest={latest}, history={history}"
    )
