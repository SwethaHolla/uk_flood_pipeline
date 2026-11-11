"""Archive the full available EA readings history to local files.

The EA Flood Monitoring API only retains a rolling ~28-day window per measure,
so this script snapshots the current window for every active measure *before*
older readings age out. It never touches BigQuery.

Layout (all under ``data/archive/`` which is gitignored):

    catalogue/measures.json      raw /id/measures envelope
    catalogue/stations.json      raw /id/stations envelope
    readings/<measure_id>.jsonl  raw reading objects, one JSON per line
    state.json                   resumable per-measure checkpoint
    readings.parquet             consolidated, de-duplicated snapshot
    report.json                  rows / span / duplicate / size summary

Resumability/incremental: each measure is written atomically (tmp -> replace)
and recorded in ``state.json`` only after the file lands on disk, so a crash
never marks unpersisted work as done. Re-running resumes from state and fetches
only readings newer than each measure's recorded max (with a 1-day overlap),
merging by reading ``@id``. Use ``--reset`` for a full re-fetch.

Freshness: the EA API retains a rolling ~28-day window. The archive was last
refreshed on 2026-11-11; re-pull before ~2026-12-09 and weekly thereafter until
the Airflow DAG owns ingestion.

Usage:
    uv run python scripts/archive_history.py --workers 6
    uv run python scripts/archive_history.py --max-requests 50   # smoke test
    uv run python scripts/archive_history.py --retry-empty       # retry 0-row measures
    uv run python scripts/archive_history.py --catalogue-only
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from uk_flood.ea_client import EAClient, EARequestError
from uk_flood.settings import get_settings

DEFAULT_OUT_DIR = Path("data/archive")
_SAFE = re.compile(r"[^A-Za-z0-9._-]+")


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def _safe_name(measure_uri: str) -> str:
    raw = measure_uri.rsplit("/", 1)[-1]
    safe = _SAFE.sub("_", raw)
    if safe != raw:
        suffix = hashlib.sha1(measure_uri.encode()).hexdigest()[:8]
        safe = f"{safe}-{suffix}"
    return safe


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def _load_state(state_file: Path) -> dict[str, Any]:
    if state_file.exists():
        try:
            return json.loads(state_file.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    return {"measures": {}}


def _save_state(state_file: Path, state: dict[str, Any]) -> None:
    state["updated_at"] = datetime.now(UTC).isoformat()
    _atomic_write_text(state_file, json.dumps(state, separators=(",", ":")))


def _select_active(
    measures: list[dict[str, Any]],
    active_only: bool,
    active_within_days: int,
) -> tuple[list[str], dict[str, Any]]:
    latest_by_uri: dict[str, datetime | None] = {}
    for m in measures:
        uri = m.get("@id")
        if not uri:
            continue
        latest = m.get("latestReading") or {}
        latest_by_uri[uri] = _parse_dt(latest.get("dateTime"))

    all_uris = list(latest_by_uri)
    stats: dict[str, Any] = {"catalogue_total": len(all_uris), "skipped_inactive": 0}
    selected = all_uris
    if active_only:
        reference = max((d for d in latest_by_uri.values() if d), default=None)
        if reference is None:
            raise RuntimeError("No latestReading dates in catalogue; cannot apply active filter")
        stats["reference"] = reference.isoformat()
        if active_within_days > 0:
            cutoff = reference - timedelta(days=active_within_days)
            selected = [u for u in all_uris if latest_by_uri[u] and latest_by_uri[u] >= cutoff]
        else:
            selected = [u for u in all_uris if latest_by_uri[u] is not None]
        stats["skipped_inactive"] = len(all_uris) - len(selected)
    stats["selected"] = len(selected)
    return selected, stats


def _fetch_measure(
    client: EAClient, measure_uri: str, since: str | None = None
) -> list[dict[str, Any]]:
    measure_id = measure_uri.rsplit("/", 1)[-1]
    params = {"since": since} if since else None
    return client.get_items(f"/id/measures/{measure_id}/readings", params)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                items.append(json.loads(line))
    return items


def _write_jsonl(path: Path, items: list[dict[str, Any]]) -> None:
    text = "\n".join(json.dumps(item, separators=(",", ":"), default=str) for item in items)
    if text:
        text += "\n"
    _atomic_write_text(path, text)


def _merge_by_id(
    existing: list[dict[str, Any]], new: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], int, bool]:
    """Merge new readings into an existing file, keyed by reading ``@id``.

    Existing order is preserved; new rows are appended. A re-fetched row with
    the same ``@id`` replaces the old one in place, so an incremental refresh
    cannot grow the file with duplicate reading IDs.
    """
    index: dict[str, int] = {}
    for i, item in enumerate(existing):
        rid = item.get("@id")
        if rid:
            index[str(rid)] = i

    added = 0
    changed = False
    for item in new:
        rid = item.get("@id")
        key = str(rid) if rid else None
        if key and key in index:
            i = index[key]
            if existing[i] != item:
                existing[i] = item
                changed = True
        else:
            existing.append(item)
            if key:
                index[key] = len(existing) - 1
            added += 1
            changed = True
    return existing, added, changed


def _since_value(record: dict[str, Any], overlap_days: int) -> str | None:
    max_dt = _parse_dt(record.get("max"))
    if max_dt is None:
        return None
    since = max_dt - timedelta(days=overlap_days)
    return since.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _fetch_and_merge(
    measure_uri: str,
    readings_dir: Path,
    pace_seconds: float,
    since: str | None,
    overwrite: bool,
) -> tuple[str, list[dict[str, Any]] | None, str | None, int]:
    """Fetch one measure and merge new readings into its raw JSONL.

    Returns (uri, merged items or None when nothing changed, error, added rows).
    A full fetch (``overwrite=True``) replaces the file; an incremental fetch
    merges by reading ``@id``.
    """
    path = readings_dir / f"{_safe_name(measure_uri)}.jsonl"
    with EAClient(get_settings()) as client:
        if pace_seconds:
            time.sleep(pace_seconds)
        try:
            items = _fetch_measure(client, measure_uri, since)
        except EARequestError as exc:
            return measure_uri, None, str(exc)[:300], 0

    if overwrite:
        _write_jsonl(path, items)
        return measure_uri, items, None, len(items)

    existing = _read_jsonl(path) if path.exists() else []
    merged, added, changed = _merge_by_id(existing, items)
    if not changed:
        return measure_uri, None, None, 0
    _write_jsonl(path, merged)
    return measure_uri, merged, None, added


def _summarize_jsonl(items: list[dict[str, Any]]) -> dict[str, Any]:
    times = [d for d in (_parse_dt(i.get("dateTime")) for i in items) if d]
    values_missing = sum(1 for i in items if i.get("value") is None)
    return {
        "rows": len(items),
        "min": min(times).isoformat() if times else None,
        "max": max(times).isoformat() if times else None,
        "value_missing": values_missing,
    }


def _write_catalogue(out_dir: Path, client: EAClient) -> dict[str, Any]:
    cat_dir = out_dir / "catalogue"
    cat_dir.mkdir(parents=True, exist_ok=True)
    measures = client.get_items("/id/measures")
    stations = client.get_items("/id/stations")
    _atomic_write_text(cat_dir / "measures.json", json.dumps(measures))
    _atomic_write_text(cat_dir / "stations.json", json.dumps(stations))
    return {"measures": measures, "stations": stations}


def _build_parquet(out_dir: Path, batch_size: int = 100) -> dict[str, Any]:
    """Consolidate raw JSONL into a de-duplicated parquet snapshot.

    Files are normalised in bounded batches so DuckDB never has to hold all
    ~5k JSONL files (and their inferred schemas) at once. Batches are written as
    intermediate parquet parts, then de-duplicated globally into the final file.
    """
    import shutil

    import duckdb

    files = sorted((out_dir / "readings").glob("*.jsonl"))
    parquet_path = out_dir / "readings.parquet"
    parts_dir = out_dir / ".parquet_parts"
    tmp_dir = out_dir / ".duckdb_tmp"
    shutil.rmtree(parts_dir, ignore_errors=True)
    shutil.rmtree(tmp_dir, ignore_errors=True)
    parts_dir.mkdir(parents=True, exist_ok=True)
    tmp_dir.mkdir(parents=True, exist_ok=True)

    def _sql_path(path: Path) -> str:
        return "'" + path.as_posix().replace("'", "''") + "'"

    con = duckdb.connect()
    try:
        con.execute("SET memory_limit='4GB'")
        con.execute(f"SET temp_directory={_sql_path(tmp_dir)}")
        con.execute("SET preserve_insertion_order=false")
        for batch_no, start in enumerate(range(0, len(files), batch_size)):
            batch = files[start : start + batch_size]
            file_list = ", ".join(_sql_path(f) for f in batch)
            con.execute(
                f"""
                CREATE OR REPLACE TEMP VIEW raw AS
                SELECT * FROM read_json_auto(
                    [{file_list}], format='newline_delimited', union_by_name=true,
                    maximum_object_size=16777216
                )
                """
            )
            cols = {r[0] for r in con.execute("DESCRIBE raw").fetchall()}
            value_expr = (
                "TRY_CAST(CAST(value AS VARCHAR) AS DOUBLE)" if "value" in cols else "NULL::DOUBLE"
            )
            qualifier_expr = (
                "CAST(qualifier AS VARCHAR)" if "qualifier" in cols else "NULL::VARCHAR"
            )
            value_is_list_expr = (
                "COALESCE(json_type(TRY_CAST(value AS JSON)) = 'ARRAY', FALSE)"
                if "value" in cols
                else "FALSE"
            )
            con.execute(
                f"""
                COPY (
                    SELECT
                        CAST("@id" AS VARCHAR) AS reading_id,
                        CAST(measure AS VARCHAR) AS measure,
                        TRY_CAST(dateTime AS TIMESTAMPTZ) AS datetime,
                        {value_expr} AS value_raw,
                        {value_is_list_expr} AS value_is_list,
                        (value IS NULL) AS value_missing,
                        {qualifier_expr} AS qualifier
                    FROM raw
                ) TO {_sql_path(parts_dir / f"part-{batch_no:05d}.parquet")}
                (FORMAT PARQUET, COMPRESSION ZSTD)
                """
            )
        parts_glob = _sql_path(parts_dir / "*.parquet")
        # Find duplicate keys with a compact hash first, then resolve the few
        # candidate groups against the full key. This avoids a 15M-row window
        # sort (which OOMs) while still de-duplicating globally across batches.
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE dup_hashes AS
            SELECT hash(measure, datetime) AS h, COUNT(*) AS n
            FROM read_parquet({parts_glob})
            GROUP BY h
            HAVING COUNT(*) > 1
            """
        )
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE dup_keys AS
            SELECT measure, datetime
            FROM read_parquet({parts_glob})
            WHERE hash(measure, datetime) IN (SELECT h FROM dup_hashes)
            GROUP BY measure, datetime
            HAVING COUNT(*) > 1
            """
        )
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE dup_keep AS
            SELECT d.measure, d.datetime, MIN(p.reading_id) AS keep_id
            FROM read_parquet({parts_glob}) p
            JOIN dup_keys d ON p.measure = d.measure AND p.datetime = d.datetime
            GROUP BY 1, 2
            """
        )
        con.execute(
            f"""
            COPY (
                SELECT p.*
                FROM read_parquet({parts_glob}) p
                LEFT JOIN dup_keep d
                    ON p.measure = d.measure AND p.datetime = d.datetime
                WHERE d.keep_id IS NULL OR p.reading_id = d.keep_id
            ) TO {_sql_path(parquet_path)} (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
        total, distinct_measures, dmin, dmax = con.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT measure), MIN(datetime), MAX(datetime) "
            f"FROM read_parquet({_sql_path(parquet_path)})"
        ).fetchone()
        total_parts = con.execute(f"SELECT COUNT(*) FROM read_parquet({parts_glob})").fetchone()[0]
        dup_groups = con.execute("SELECT COUNT(*) FROM dup_keys").fetchone()[0]
        dup_extra = int(total_parts) - int(total)
    finally:
        con.close()
        shutil.rmtree(parts_dir, ignore_errors=True)
        shutil.rmtree(tmp_dir, ignore_errors=True)
    return {
        "parquet_rows": int(total),
        "parquet_distinct_measures": int(distinct_measures),
        "parquet_min": str(dmin) if dmin else None,
        "parquet_max": str(dmax) if dmax else None,
        "parquet_bytes": parquet_path.stat().st_size if parquet_path.exists() else 0,
        "duplicate_key_groups": int(dup_groups),
        "duplicate_extra_rows": int(dup_extra),
    }


def _dir_size(path: Path) -> int:
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def archive(
    out_dir: Path,
    workers: int,
    pace_seconds: float,
    active_only: bool,
    active_within_days: int,
    measure_limit: int | None,
    max_requests: int | None,
    retry_empty: bool,
    reset: bool,
    catalogue_only: bool,
    overlap_days: int = 1,
    skip_report: bool = False,
) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    readings_dir = out_dir / "readings"
    readings_dir.mkdir(parents=True, exist_ok=True)
    state_file = out_dir / "state.json"
    state = {"measures": {}} if reset else _load_state(state_file)
    done: dict[str, Any] = state.get("measures", {})

    client = EAClient(get_settings())
    try:
        print(f"state: {state_file.resolve()} ({len(done)} measures recorded)")
        cat = _write_catalogue(out_dir, client)
    finally:
        client.close()
    measures = cat["measures"]
    selected, stats = _select_active(measures, active_only, active_within_days)
    if measure_limit:
        selected = selected[:measure_limit]
    print(
        f"catalogue={stats['catalogue_total']} selected={stats['selected']} "
        f"skipped_inactive={stats['skipped_inactive']}"
        + (f" reference={stats.get('reference')}" if stats.get("reference") else "")
    )

    if catalogue_only:
        print("catalogue-only: wrote measures.json + stations.json, no readings fetched")
        return 0

    def should_skip(uri: str) -> bool:
        rec = done.get(uri)
        path = readings_dir / f"{_safe_name(uri)}.jsonl"
        if not rec or not path.exists():
            return False
        return bool(rec.get("empty")) and not retry_empty

    todo = [u for u in selected if not should_skip(u)]
    if max_requests is not None:
        todo = todo[:max_requests]
    skipped = len(selected) - len([u for u in selected if not should_skip(u)])
    print(f"skipped_empty={skipped} to_check={len(todo)}")
    if not todo:
        print("nothing to check")
        if not skip_report:
            _report(out_dir, state, stats)
        return 0

    jobs: list[tuple[str, str | None, bool]] = []
    for uri in todo:
        rec = done.get(uri)
        path = readings_dir / f"{_safe_name(uri)}.jsonl"
        overwrite = not rec or not path.exists() or (rec.get("empty") and retry_empty)
        since = None if overwrite else _since_value(rec, overlap_days)
        if since is None:
            overwrite = True
        jobs.append((uri, since, overwrite))

    lock = threading.Lock()
    failures: list[str] = []
    empty: list[str] = []
    updated = 0
    no_change = 0
    started = time.time()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_fetch_and_merge, uri, readings_dir, pace_seconds, since, overwrite): uri
            for uri, since, overwrite in jobs
        }
        for i, future in enumerate(as_completed(futures), start=1):
            uri, items, error, added = future.result()
            with lock:
                if error is not None:
                    failures.append(uri)
                    print(f"  FAIL {uri}: {error}", file=sys.stderr)
                elif items is None:
                    rec = done.get(uri)
                    if rec is not None:
                        rec["last_checked_at"] = datetime.now(UTC).isoformat()
                    no_change += 1
                else:
                    summary = _summarize_jsonl(items)
                    summary["empty"] = not items
                    summary["fetched_at"] = datetime.now(UTC).isoformat()
                    summary["last_checked_at"] = summary["fetched_at"]
                    summary["added_rows"] = added
                    summary["bytes"] = (readings_dir / f"{_safe_name(uri)}.jsonl").stat().st_size
                    done[uri] = summary
                    updated += 1
                    if not items:
                        empty.append(uri)
                if i % 25 == 0 or i == len(todo):
                    _save_state(state_file, {"measures": done})
                    elapsed = time.time() - started
                    rate = i / elapsed if elapsed else 0
                    print(
                        f"  progress: {i}/{len(todo)} "
                        f"({rate:.1f}/s, updated={updated}, no_change={no_change}, "
                        f"{len(failures)} failed, {len(empty)} empty)"
                    )

    _save_state(state_file, {"measures": done})
    print(
        f"checked={len(todo)} updated={updated} no_change={no_change} "
        f"failed={len(failures)} empty={len(empty)}"
    )
    if failures:
        print("failed measures (will retry next run):", file=sys.stderr)
        for uri in failures[:20]:
            print(f"  {uri}", file=sys.stderr)

    if not skip_report:
        _report(out_dir, {"measures": done}, stats)
    return 0 if not failures else 1


def _report(out_dir: Path, state: dict[str, Any], stats: dict[str, Any]) -> dict[str, Any]:
    done = state.get("measures", {})
    total_rows = sum(int(r.get("rows", 0)) for r in done.values())
    mins = [r["min"] for r in done.values() if r.get("min")]
    maxs = [r["max"] for r in done.values() if r.get("max")]
    print("== building parquet snapshot ==")
    parquet = _build_parquet(out_dir)
    jsonl_bytes = _dir_size(out_dir / "readings")
    total_bytes = _dir_size(out_dir)
    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "catalogue": stats,
        "measures_archived": len(done),
        "measures_empty": sum(1 for r in done.values() if r.get("empty")),
        "raw_rows": total_rows,
        "raw_date_min": min(mins) if mins else None,
        "raw_date_max": max(maxs) if maxs else None,
        "jsonl_bytes": jsonl_bytes,
        "total_bytes": total_bytes,
        **parquet,
    }
    _atomic_write_text(out_dir / "report.json", json.dumps(report, indent=2))
    print("== archive summary ==")
    print(
        f"  measures archived : {report['measures_archived']:,} ({report['measures_empty']} empty)"
    )
    print(f"  raw rows          : {report['raw_rows']:,}")
    print(f"  raw date span     : {report['raw_date_min']} -> {report['raw_date_max']}")
    print(
        f"  parquet rows      : {report['parquet_rows']:,} "
        f"(distinct measures {report['parquet_distinct_measures']:,})"
    )
    print(f"  parquet date span : {report['parquet_min']} -> {report['parquet_max']}")
    print(f"  jsonl bytes       : {jsonl_bytes:,} ({jsonl_bytes / 1024**2:.1f} MiB)")
    print(
        f"  parquet bytes     : {report['parquet_bytes']:,} "
        f"({report['parquet_bytes'] / 1024**2:.1f} MiB)"
    )
    print(f"  total disk        : {total_bytes:,} ({total_bytes / 1024**2:.1f} MiB)")
    print(f"  duplicate key groups (measure, datetime): {report['duplicate_key_groups']:,}")
    print(f"  duplicate extra rows removed: {report['duplicate_extra_rows']:,}")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--pace-seconds", type=float, default=0.05)
    parser.add_argument(
        "--all-measures",
        action="store_true",
        help="Archive every measure, including those with no latestReading.",
    )
    parser.add_argument(
        "--active-within-days",
        type=int,
        default=0,
        help="Only measures with latestReading within N days of the newest (0 = any).",
    )
    parser.add_argument("--measure-limit", type=int, default=None)
    parser.add_argument("--max-requests", type=int, default=None)
    parser.add_argument("--retry-empty", action="store_true")
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("--catalogue-only", action="store_true")
    parser.add_argument(
        "--overlap-days",
        type=int,
        default=1,
        help="On incremental runs, re-fetch this many days before each measure's max.",
    )
    parser.add_argument(
        "--skip-report",
        action="store_true",
        help="Fetch only; skip the parquet rebuild/report (use for intermediate resume chunks).",
    )
    args = parser.parse_args()
    return archive(
        out_dir=args.out_dir,
        workers=args.workers,
        pace_seconds=args.pace_seconds,
        active_only=not args.all_measures,
        active_within_days=args.active_within_days,
        measure_limit=args.measure_limit,
        max_requests=args.max_requests,
        retry_empty=args.retry_empty,
        reset=args.reset,
        catalogue_only=args.catalogue_only,
        overlap_days=args.overlap_days,
        skip_report=args.skip_report,
    )


if __name__ == "__main__":
    raise SystemExit(main())
