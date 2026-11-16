"""Incremental fact rebuild must handle late/backfilled old-dated rows."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
DBT_DIR = REPO_ROOT / "dbt"
DBT = Path(sys.executable).with_name("dbt.exe" if os.name == "nt" else "dbt")


def _run_dbt(db_path: Path, *args: str, lookback_days: int) -> None:
    if not DBT.exists():
        pytest.skip("dbt executable is not installed")

    if not (DBT_DIR / "dbt_packages").exists():
        deps = subprocess.run(
            [
                str(DBT),
                "deps",
                "--profiles-dir",
                str(DBT_DIR),
                "--project-dir",
                str(DBT_DIR),
            ],
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
        )
        if deps.returncode != 0:
            pytest.fail(f"dbt deps failed\nSTDOUT:\n{deps.stdout}\nSTDERR:\n{deps.stderr}")

    env = os.environ.copy()
    env["DUCKDB_PATH"] = str(db_path)
    env["DBT_SOURCE_DATABASE"] = db_path.stem
    proc = subprocess.run(
        [
            str(DBT),
            *args,
            "--profiles-dir",
            str(DBT_DIR),
            "--project-dir",
            str(DBT_DIR),
            "--target",
            "local",
            "--vars",
            json.dumps({"fct_lookback_days": lookback_days}),
        ],
        cwd=REPO_ROOT,
        env=env,
        text=True,
        capture_output=True,
    )
    if proc.returncode != 0:
        pytest.fail(f"dbt {' '.join(args)} failed\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}")


def _seed_raw(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("create schema if not exists uk_flood_raw")
    con.execute(
        """
        create table uk_flood_raw.stations (
            id varchar, station_reference varchar, label varchar, town varchar,
            river_name varchar, catchment_name varchar, rloi_id varchar,
            status varchar, lat double, long double, easting double,
            northing double, date_opened date, _dlt_load_id varchar, _dlt_id varchar
        )
        """
    )
    con.execute(
        """
        create table uk_flood_raw.stations__measures (
            _dlt_parent_id varchar, value varchar
        )
        """
    )
    con.execute(
        """
        create table uk_flood_raw.measures (
            id varchar, label varchar, unit varchar, unit_name varchar,
            datum_type varchar, qualifier varchar, period varchar, notation varchar,
            latest_reading_value double, latest_reading_datetime timestamp,
            latest_reading_is_list boolean, latest_reading_missing boolean,
            _dlt_load_id varchar, _dlt_id varchar
        )
        """
    )
    con.execute(
        """
        create table uk_flood_raw.readings (
            id varchar, measure varchar, datetime timestamp, value_raw double,
            value_is_list boolean, value_missing boolean, qualifier varchar,
            _dlt_load_id varchar, _dlt_id varchar
        )
        """
    )
    con.execute(
        """
        insert into uk_flood_raw.stations values
        ('s1', 'ref', 'Station', 'Town', 'River', 'Catchment', '1', 'Active',
         51.0, -1.0, 100.0, 200.0, date '2020-01-01', 'load-1', 'station-1')
        """
    )
    con.execute("insert into uk_flood_raw.stations__measures values ('station-1', 'm1')")
    con.execute(
        """
        insert into uk_flood_raw.measures values
        ('m1', 'Level', 'm', 'mASD', 'mASD', 'Ground', '15min', 'Instantaneous',
         1.0, timestamp '2026-01-10 01:00:00', false, false, 'load-1', 'measure-1')
        """
    )
    con.execute(
        """
        insert into uk_flood_raw.readings values
        ('r1', 'm1', timestamp '2026-01-10 00:00:00', 1.0, false, false, null,
         'load-1', 'reading-1'),
        ('r2', 'm1', timestamp '2026-01-10 01:00:00', 1.1, false, false, null,
         'load-1', 'reading-2')
        """
    )


def _fct_schema(con: duckdb.DuckDBPyConnection) -> str:
    row = con.execute(
        "select table_schema from information_schema.tables where table_name = 'fct_readings'"
    ).fetchone()
    assert row is not None, "dbt did not create fct_readings"
    return str(row[0])


def _fct_count(con: duckdb.DuckDBPyConnection, schema: str) -> int:
    return int(con.execute(f'select count(*) from "{schema}".fct_readings').fetchone()[0])


def test_backfilled_old_row_needs_widened_lookback(tmp_path: Path) -> None:
    db_path = tmp_path / "incremental.duckdb"
    con = duckdb.connect(str(db_path))
    try:
        _seed_raw(con)
    finally:
        con.close()

    _run_dbt(db_path, "run", "--select", "+fct_readings", lookback_days=3)

    con = duckdb.connect(str(db_path))
    try:
        schema = _fct_schema(con)
        assert _fct_count(con, schema) == 2
        con.execute(
            """
            insert into uk_flood_raw.readings values
            ('r-backfill', 'm1', timestamp '2026-01-01 00:00:00', 0.9, false, false,
             null, 'load-2', 'reading-backfill')
            """
        )
    finally:
        con.close()

    _run_dbt(db_path, "run", "--select", "+fct_readings", lookback_days=3)
    con = duckdb.connect(str(db_path))
    try:
        schema = _fct_schema(con)
        assert _fct_count(con, schema) == 2
        backfilled = con.execute(
            f"select count(*) from \"{schema}\".fct_readings where reading_id = 'r-backfill'"
        ).fetchone()[0]
        assert backfilled == 0
    finally:
        con.close()

    _run_dbt(db_path, "run", "--select", "+fct_readings", lookback_days=30)
    con = duckdb.connect(str(db_path))
    try:
        schema = _fct_schema(con)
        assert _fct_count(con, schema) == 3
        backfilled = con.execute(
            f"select count(*) from \"{schema}\".fct_readings where reading_id = 'r-backfill'"
        ).fetchone()[0]
        assert backfilled == 1
    finally:
        con.close()
