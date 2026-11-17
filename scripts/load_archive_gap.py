"""Append local archive readings to BigQuery with a free load job.

The local archive is the source of truth for the 2026-11-12..2026-11-17 gap.
This script finds the newest raw partition from INFORMATION_SCHEMA.PARTITIONS,
uses the exact MAX(datetime) in that partition as the boundary, filters the
archive to boundary minus a small overlap, adds deterministic synthetic
``_dlt_load_id``/``_dlt_id`` values, and appends the result with a BigQuery
load job (load jobs are free; no query-quota bytes).

Usage:
    uv run python scripts/load_archive_gap.py --dry-run
    uv run python scripts/load_archive_gap.py
    uv run python scripts/load_archive_gap.py --verify
"""

from __future__ import annotations

import argparse
import json
import statistics
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb
from google.cloud import bigquery

from uk_flood.settings import get_settings

DEFAULT_ARCHIVE = Path("data/archive/readings.parquet")
READINGS_SCHEMA = [
    bigquery.SchemaField("id", "STRING"),
    bigquery.SchemaField("measure", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("datetime", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("value_raw", "FLOAT"),
    bigquery.SchemaField("value_is_list", "BOOLEAN"),
    bigquery.SchemaField("value_missing", "BOOLEAN"),
    bigquery.SchemaField("qualifier", "STRING"),
    bigquery.SchemaField("_resource", "STRING"),
    bigquery.SchemaField("_dlt_load_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("_dlt_id", "STRING", mode="REQUIRED"),
]


def _sql_path(path: Path) -> str:
    return "'" + path.as_posix().replace("'", "''") + "'"


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _table_ref(project: str, dataset: str, table: str) -> str:
    return f"`{project}.{dataset}.{table}`"


def _raw_partition_counts(
    client: bigquery.Client, project: str, dataset: str, table: str
) -> dict[str, int]:
    sql = f"""
        SELECT partition_id, total_rows
        FROM `{project}.{dataset}.INFORMATION_SCHEMA.PARTITIONS`
        WHERE table_name = {_sql_literal(table)} AND partition_id IS NOT NULL
    """
    out: dict[str, int] = {}
    for row in client.query(sql).result():
        pid = str(row["partition_id"])
        out[f"{pid[:4]}-{pid[4:6]}-{pid[6:8]}"] = int(row["total_rows"] or 0)
    return out


def _detect_gap_start(counts: dict[str, int], window_days: int, threshold: float) -> dict[str, Any]:
    dates = sorted(counts)
    if not dates:
        raise RuntimeError("no partitions found")
    latest = dates[-1]
    window = dates[-window_days:]
    baseline_days = [d for d in window if d != latest and counts[d] > 0]
    median = statistics.median([counts[d] for d in baseline_days])
    gap_start = next(
        (d for d in window if d != latest and counts[d] < threshold * median),
        latest,
    )
    return {
        "latest_partition": latest,
        "window_start": window[0],
        "window_days": len(window),
        "median_rows": median,
        "threshold": threshold,
        "gap_start": gap_start,
        "gap_start_rows": counts.get(gap_start, 0),
    }


def _raw_boundary(
    client: bigquery.Client, project: str, dataset: str, table: str, threshold: float
) -> dict[str, Any]:
    counts = _raw_partition_counts(client, project, dataset, table)
    gap = _detect_gap_start(counts, window_days=30, threshold=threshold)
    partition_id = gap["latest_partition"].replace("-", "")
    max_sql = f"""
        SELECT MAX(datetime) AS max_dt, COUNT(*) AS row_count
        FROM {_table_ref(project, dataset, table)}
        WHERE DATE(datetime) = DATE {_sql_literal(gap["latest_partition"])}
    """
    max_row = next(iter(client.query(max_sql).result()))
    gap_start_dt = datetime.fromisoformat(gap["gap_start"]).replace(tzinfo=UTC)
    previous_day = (gap_start_dt - timedelta(days=1)).date().isoformat()
    previous_sql = f"""
        SELECT MAX(datetime) AS max_dt
        FROM {_table_ref(project, dataset, table)}
        WHERE DATE(datetime) = DATE {_sql_literal(previous_day)}
    """
    previous_row = next(iter(client.query(previous_sql).result()))
    return {
        **gap,
        "partition_id": partition_id,
        "partition_rows": counts.get(gap["latest_partition"], 0),
        "max_datetime": max_row["max_dt"],
        "max_partition_rows": max_row["row_count"],
        "previous_day": previous_day,
        "previous_day_max": previous_row["max_dt"],
    }


def _archive_bounds(con: duckdb.DuckDBPyConnection, archive: Path) -> dict[str, Any]:
    row = con.execute(
        f"""
        SELECT MIN(datetime) AS min_dt, MAX(datetime) AS max_dt, COUNT(*) AS rows
        FROM read_parquet({_sql_path(archive)})
        """
    ).fetchone()
    return {"min_datetime": row[0], "max_datetime": row[1], "rows": row[2]}


def _build_filtered_parquet(
    con: duckdb.DuckDBPyConnection,
    archive: Path,
    output: Path,
    since: datetime,
    load_id: str,
) -> dict[str, Any]:
    con.execute(
        f"""
        COPY (
            SELECT
                CAST(reading_id AS VARCHAR) AS id,
                CAST(measure AS VARCHAR) AS measure,
                CAST(datetime AS TIMESTAMPTZ) AS datetime,
                CAST(value_raw AS DOUBLE) AS value_raw,
                CAST(value_is_list AS BOOLEAN) AS value_is_list,
                CAST(value_missing AS BOOLEAN) AS value_missing,
                CAST(qualifier AS VARCHAR) AS qualifier,
                'readings' AS _resource,
                {_sql_literal(load_id)} AS _dlt_load_id,
                md5(concat_ws('|', CAST(measure AS VARCHAR), CAST(datetime AS VARCHAR), CAST(reading_id AS VARCHAR))) AS _dlt_id
            FROM read_parquet({_sql_path(archive)})
            WHERE datetime >= TIMESTAMPTZ {_sql_literal(since.astimezone(UTC).isoformat())}
        ) TO {_sql_path(output)} (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    row = con.execute(
        f"""
        SELECT
            COUNT(*) AS rows,
            MIN(datetime) AS min_dt,
            MAX(datetime) AS max_dt,
            COUNT(DISTINCT measure) AS measures,
            COUNT(DISTINCT _dlt_id) AS ids,
            COUNT(*) - COUNT(DISTINCT concat_ws('|', measure, CAST(datetime AS VARCHAR))) AS duplicate_keys,
            COUNT(*) FILTER (WHERE measure IS NULL OR datetime IS NULL OR _dlt_id IS NULL OR _dlt_load_id IS NULL) AS null_required
        FROM read_parquet({_sql_path(output)})
        """
    ).fetchone()
    return {
        "rows": row[0],
        "min_datetime": row[1],
        "max_datetime": row[2],
        "measures": row[3],
        "distinct_dlt_ids": row[4],
        "duplicate_keys": row[5],
        "null_required": row[6],
    }


def _load_parquet(
    client: bigquery.Client,
    path: Path,
    project: str,
    dataset: str,
    table: str,
    location: str,
) -> dict[str, Any]:
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=READINGS_SCHEMA,
        autodetect=False,
    )
    table_ref = f"{project}.{dataset}.{table}"
    with path.open("rb") as fh:
        job = client.load_table_from_file(fh, table_ref, job_config=job_config, location=location)
    job.result()
    return {
        "job_id": job.job_id,
        "output_rows": job.output_rows,
        "total_bytes_processed": getattr(job, "total_bytes_processed", None),
        "total_bytes_billed": getattr(job, "total_bytes_billed", None),
        "destination": table_ref,
    }


def _archive_day_counts(con: duckdb.DuckDBPyConnection, archive: Path) -> dict[str, int]:
    rows = con.execute(
        f"""
        SELECT CAST(DATE(datetime) AS VARCHAR) AS day, COUNT(*) AS n
        FROM read_parquet({_sql_path(archive)})
        GROUP BY 1
        ORDER BY 1
        """
    ).fetchall()
    return {str(day): int(n) for day, n in rows}


def verify_coverage(
    client: bigquery.Client,
    con: duckdb.DuckDBPyConnection,
    archive: Path,
    project: str,
    dataset: str,
    table: str,
    threshold: float,
) -> dict[str, Any]:
    archive_counts = _archive_day_counts(con, archive)
    raw_counts = _raw_partition_counts(client, project, dataset, table)
    max_archive_day = max(archive_counts)
    complete_days = [d for d in sorted(archive_counts) if d < max_archive_day]
    archive_values = [archive_counts[d] for d in complete_days if archive_counts[d] > 0]
    raw_values = [raw_counts.get(d, 0) for d in complete_days if archive_counts[d] > 0]
    median = statistics.median(archive_values)
    median_raw = statistics.median(raw_values)
    recent = complete_days[-30:]
    recent_median = statistics.median([archive_counts[d] for d in recent if archive_counts[d] > 0])
    recent_median_raw = statistics.median([raw_counts.get(d, 0) for d in recent])
    flagged: list[dict[str, Any]] = []
    for day in complete_days:
        expected = archive_counts[day]
        actual = raw_counts.get(day, 0)
        ratio = actual / expected if expected else None
        if actual == 0 or (ratio is not None and ratio < threshold):
            flagged.append(
                {
                    "day": day,
                    "raw": actual,
                    "archive": expected,
                    "ratio": round(ratio, 4) if ratio is not None else None,
                }
            )
    recent_set = set(recent)
    low_vs_recent_median = [
        {
            "day": day,
            "raw": raw_counts.get(day, 0),
            "median_raw": recent_median_raw,
            "ratio": round(raw_counts.get(day, 0) / recent_median_raw, 4)
            if recent_median_raw
            else None,
        }
        for day in recent
        if raw_counts.get(day, 0) == 0
        or (recent_median_raw and raw_counts.get(day, 0) < threshold * recent_median_raw)
    ]
    return {
        "archive_min": min(archive_counts),
        "archive_max": max(archive_counts),
        "complete_days": len(complete_days),
        "median_archive_rows": median,
        "median_raw_rows": median_raw,
        "median_archive_rows_recent_30d": recent_median,
        "median_raw_rows_recent_30d": recent_median_raw,
        "threshold": threshold,
        "flagged": flagged,
        "flagged_recent_30d": [f for f in flagged if f["day"] in recent_set],
        "low_vs_recent_median": low_vs_recent_median,
        "raw_days": len(raw_counts),
        "archive_days": len(archive_counts),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive-parquet", type=Path, default=DEFAULT_ARCHIVE)
    parser.add_argument("--project-id", default=None)
    parser.add_argument("--dataset", default=None)
    parser.add_argument("--table", default="readings")
    parser.add_argument("--overlap-days", type=int, default=0)
    parser.add_argument("--overlap-hours", type=int, default=1)
    parser.add_argument("--load-id", default=None)
    parser.add_argument("--output-parquet", type=Path, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--threshold", type=float, default=0.8)
    args = parser.parse_args()

    settings = get_settings()
    project = args.project_id or settings.gcp_project_id
    dataset = args.dataset or settings.bq_raw_dataset
    archive = args.archive_parquet
    if not archive.exists():
        raise SystemExit(f"archive not found: {archive}")

    client = bigquery.Client(project=project)
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    try:
        if args.verify:
            result = verify_coverage(
                client, con, archive, project, dataset, args.table, args.threshold
            )
            print(json.dumps(result, indent=2, default=str))
            return 0 if not result["flagged"] else 1

        bounds = _archive_bounds(con, archive)
        boundary = _raw_boundary(client, project, dataset, args.table, args.threshold)
        gap_start = datetime.fromisoformat(boundary["gap_start"]).replace(tzinfo=UTC)
        if boundary["previous_day_max"] is not None:
            since = boundary["previous_day_max"] - timedelta(
                days=args.overlap_days, hours=args.overlap_hours
            )
        else:
            since = gap_start - timedelta(days=args.overlap_days, hours=args.overlap_hours)
        load_id = (
            args.load_id or f"archive-{bounds['max_datetime'].astimezone(UTC):%Y-%m-%dT%H:%M:%SZ}"
        )
        output = args.output_parquet or Path(tempfile.gettempdir()) / "uk_flood_archive_gap.parquet"
        output.unlink(missing_ok=True)
        summary = _build_filtered_parquet(con, archive, output, since, load_id)
        report = {
            "archive": str(archive),
            "archive_bounds": {k: str(v) for k, v in bounds.items()},
            "raw_boundary": {k: str(v) for k, v in boundary.items()},
            "since": since.astimezone(UTC).isoformat(),
            "load_id": load_id,
            "filtered": {k: str(v) for k, v in summary.items()},
            "output_parquet": str(output),
            "output_bytes": output.stat().st_size,
            "table": f"{project}.{dataset}.{args.table}",
            "schema": [f"{f.name}:{f.field_type}:{f.mode}" for f in READINGS_SCHEMA],
        }
        print(json.dumps(report, indent=2, default=str))
        if args.dry_run:
            sample = con.execute(
                f"SELECT * FROM read_parquet({_sql_path(output)}) LIMIT 3"
            ).fetchall()
            print("sample_rows")
            for row in sample:
                print(row)
            return 0
        if summary["duplicate_keys"] or summary["null_required"]:
            raise SystemExit("refusing to load: duplicate keys or null required fields")
        loaded = _load_parquet(client, output, project, dataset, args.table, settings.bq_location)
        print(json.dumps(loaded, indent=2, default=str))
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
