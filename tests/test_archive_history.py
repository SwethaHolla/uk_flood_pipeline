"""Offline tests for the local EA readings archive."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from scripts.archive_history import _build_parquet, _merge_by_id, _since_value


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(json.dumps(row, separators=(",", ":")) for row in rows) + "\n",
        encoding="utf-8",
    )


def test_build_parquet_batches_and_dedups(tmp_path: Path) -> None:
    readings = tmp_path / "readings"
    _write_jsonl(
        readings / "file1.jsonl",
        [
            {
                "@id": "a",
                "measure": "m1",
                "dateTime": "2026-01-01T00:00:00Z",
                "value": 1.0,
                "qualifier": "Good",
            }
        ],
    )
    _write_jsonl(
        readings / "file2.jsonl",
        [
            {
                "@id": "b",
                "measure": "m1",
                "dateTime": "2026-01-01T01:00:00+01:00",
                "value": 2.0,
                "qualifier": "Good",
            }
        ],
    )
    _write_jsonl(
        readings / "file3.jsonl",
        [
            {
                "@id": "c",
                "measure": "m2",
                "dateTime": "2026-01-02T00:00:00Z",
                "value": [1, 2],
            }
        ],
    )
    _write_jsonl(
        readings / "file4.jsonl",
        [
            {
                "@id": "d",
                "measure": "m2",
                "dateTime": "2026-01-03T00:00:00Z",
                "value": None,
                "qualifier": "Missing",
            }
        ],
    )

    report = _build_parquet(tmp_path, batch_size=1)

    assert report["parquet_rows"] == 3
    assert report["parquet_distinct_measures"] == 2
    assert report["duplicate_key_groups"] == 1
    assert report["duplicate_extra_rows"] == 1

    parquet = (tmp_path / "readings.parquet").as_posix()
    con = duckdb.connect()
    try:
        rows = con.execute(
            f"""
            SELECT reading_id, measure, CAST(datetime AS VARCHAR), value_raw,
                   value_is_list, value_missing, qualifier
            FROM read_parquet('{parquet}')
            ORDER BY measure, datetime
            """
        ).fetchall()
    finally:
        con.close()

    assert rows[0][0:2] == ("a", "m1")
    assert rows[0][3] == 1.0
    assert rows[0][4] is False
    assert rows[0][5] is False
    assert rows[0][6] == "Good"

    assert rows[1][0:2] == ("c", "m2")
    assert rows[1][3] is None
    assert rows[1][4] is True
    assert rows[1][5] is False
    assert rows[1][6] is None

    assert rows[2][0:2] == ("d", "m2")
    assert rows[2][3] is None
    assert rows[2][4] is False
    assert rows[2][5] is True
    assert rows[2][6] == "Missing"


def test_build_parquet_preserves_utc_across_dst(tmp_path: Path) -> None:
    readings = tmp_path / "readings"
    _write_jsonl(
        readings / "dst.jsonl",
        [
            {
                "@id": "a",
                "measure": "m1",
                "dateTime": "2025-03-30T01:00:00Z",
                "value": 0.29,
            },
            {
                "@id": "b",
                "measure": "m1",
                "dateTime": "2025-03-30T02:00:00Z",
                "value": 0.289,
            },
        ],
    )

    report = _build_parquet(tmp_path, batch_size=1)

    assert report["parquet_rows"] == 2
    assert report["duplicate_key_groups"] == 0
    assert report["duplicate_extra_rows"] == 0

    parquet = (tmp_path / "readings.parquet").as_posix()
    con = duckdb.connect()
    try:
        con.execute("SET TimeZone='UTC'")
        rows = con.execute(
            f"""
            SELECT reading_id, CAST(datetime AS VARCHAR)
            FROM read_parquet('{parquet}')
            ORDER BY datetime
            """
        ).fetchall()
    finally:
        con.close()

    assert rows[0][0] == "a"
    assert rows[0][1].startswith("2025-03-30 01:00:00")
    assert rows[1][0] == "b"
    assert rows[1][1].startswith("2025-03-30 02:00:00")


def test_merge_by_id_updates_existing_and_appends_new() -> None:
    existing = [{"@id": "a", "value": 1}, {"@id": "b", "value": 2}]
    new = [{"@id": "b", "value": 3}, {"@id": "c", "value": 4}]

    merged, added, changed = _merge_by_id(existing, new)

    assert added == 1
    assert changed is True
    assert [row["@id"] for row in merged] == ["a", "b", "c"]
    assert merged[1]["value"] == 3

    merged_again, added_again, changed_again = _merge_by_id(merged, [{"@id": "b", "value": 3}])
    assert added_again == 0
    assert changed_again is False
    assert merged_again == merged


def test_since_value_uses_overlap() -> None:
    assert _since_value({"max": "2026-11-11T21:00:00+00:00"}, 1) == "2026-11-10T21:00:00Z"
