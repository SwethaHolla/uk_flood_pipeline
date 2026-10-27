"""Seed ~4 weeks of readings history from the live EA API.

The global /data/readings endpoint caps responses at 10 000 rows and rejects
``?since=`` (PRD §7 quirk 10), so history is pulled measure-by-measure
(``?measure=<uri>`` returns each measure's full ~4-week window in one call).

- concurrent with a small worker pool (be nice to a beta API)
- resumable: only measures whose rows have been **loaded** are checkpointed, so a
  crash never marks unpersisted work as done (completed+loaded rows are re-fetched
  after a crash, which is safe because the load is an idempotent merge)
- active filter: the EA API exposes no explicit ``status`` field, so "active" is
  inferred from ``latestReading.dateTime`` being within N days of the newest
  reading seen in the measure catalogue
- every page is contract-validated and merged idempotently into ``readings``, so
  re-running never creates duplicates

Usage:
    uv run python scripts/seed_history.py --destination duckdb
    uv run python scripts/seed_history.py --destination bigquery --workers 8
    uv run python scripts/seed_history.py --destination bigquery --active-only
    uv run python scripts/seed_history.py --measure-limit 100 --active-only \\
        --destination duckdb --state-file data/pilot_state.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from uk_flood.ea_client import EAClient, EARequestError
from uk_flood.pipelines import load_readings_history
from uk_flood.settings import get_settings

DEFAULT_STATE_FILE = Path("data/seed_history_state.json")


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def _load_state(state_file: Path) -> dict:
    if state_file.exists():
        return json.loads(state_file.read_text(encoding="utf-8"))
    return {"completed_measures": []}


def _save_state(state_file: Path, state: dict) -> None:
    """Atomic write so a crash mid-save cannot corrupt the checkpoint."""
    state_file.parent.mkdir(parents=True, exist_ok=True)
    tmp = state_file.with_suffix(state_file.suffix + ".tmp")
    tmp.write_text(json.dumps(state), encoding="utf-8")
    os.replace(tmp, state_file)


def _select_measures(
    measures: list[dict],
    measure_limit: int | None,
    active_only: bool,
    active_within_days: int,
) -> tuple[list[str], dict[str, Any]]:
    """Return (selected measure URIs, stats).

    The EA API has no ``status`` field. Verified empirically (2026-10-25):
    - measures with ``latestReading == null`` return **0** historical rows;
    - measures with a stale (old) ``latestReading`` still return partial history.

    So ``active_only`` means "has any reading" (latestReading non-null) and keeps
    stale-but-partial history. Pass ``active_within_days > 0`` for a stricter
    freshness cutoff relative to the newest reading in the catalogue.
    """
    latest_by_uri: dict[str, datetime | None] = {}
    period_by_uri: dict[str, int | None] = {}
    for m in measures:
        uri = m.get("@id")
        if not uri:
            continue
        latest = m.get("latestReading") or {}
        latest_by_uri[uri] = _parse_dt(latest.get("dateTime"))
        period_by_uri[uri] = m.get("period")

    all_uris = list(latest_by_uri)
    stats: dict[str, Any] = {"total": len(all_uris), "skipped_inactive": 0}
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
    if measure_limit:
        selected = selected[:measure_limit]
    # Approximate rows/day from the measures' declared cadence (period seconds).
    stats["est_rows_day"] = round(
        sum(86400.0 / p for u in selected if (p := period_by_uri.get(u)) and p > 0)
    )
    stats["selected"] = len(selected)
    return selected, stats


def fetch_measure_readings(client: EAClient, measure_uri: str) -> list[dict]:
    """One measure's full history via the per-measure readings endpoint.

    The EA API rejects ``?count=`` on ``/data/readings`` (HTTP 400) and that
    endpoint does not accept ``?measure=`` filtering (returns 0). The working
    route is ``/id/measures/{id}/readings`` which returns the full ~4-week window.
    """
    measure_id = measure_uri.rsplit("/", 1)[-1]
    try:
        return client.get_items(f"/id/measures/{measure_id}/readings")
    except EARequestError as exc:
        print(f"  WARN {measure_uri}: {exc}", file=sys.stderr)
        return []


def summarize(destination: str, duckdb_path: str) -> None:
    """Print a post-load integrity summary (per-measure coverage + duplicates)."""
    if destination == "duckdb":
        import duckdb

        con = duckdb.connect(duckdb_path)
        try:
            total, distinct_m = con.execute(
                "SELECT COUNT(*), COUNT(DISTINCT measure) FROM uk_flood_raw.readings"
            ).fetchone()
            pmin, pmed, pmax = con.execute(
                "SELECT MIN(span), MEDIAN(span), MAX(span) FROM ("
                " SELECT measure, DATE_DIFF('day', MIN(datetime), MAX(datetime)) span"
                " FROM uk_flood_raw.readings GROUP BY measure)"
            ).fetchone()
            cmed = con.execute(
                "SELECT MEDIAN(c) FROM (SELECT measure, COUNT(*) c FROM uk_flood_raw.readings "
                "GROUP BY measure)"
            ).fetchone()[0]
            dups = con.execute(
                "SELECT COUNT(*) FROM (SELECT measure, datetime FROM uk_flood_raw.readings "
                "GROUP BY measure, datetime HAVING COUNT(*) > 1)"
            ).fetchone()[0]
        finally:
            con.close()
    else:
        from google.cloud import bigquery

        settings = get_settings()
        client = bigquery.Client(project=settings.gcp_project_id)
        t = f"`{settings.gcp_project_id}.{settings.bq_raw_dataset}.readings`"
        total, distinct_m = next(
            iter(client.query(f"SELECT COUNT(*), COUNT(DISTINCT measure) FROM {t}").result())
        )
        pmin, pmed, pmax = next(
            iter(
                client.query(
                    "SELECT MIN(span), APPROX_QUANTILES(span, 2)[OFFSET(1)], MAX(span) FROM ("
                    f" SELECT measure, DATE_DIFF(MAX(DATE(datetime)), MIN(DATE(datetime)), DAY) span"
                    f" FROM {t} GROUP BY measure)"
                ).result()
            )
        )
        cmed = next(
            iter(
                client.query(
                    "SELECT APPROX_QUANTILES(c, 2)[OFFSET(1)] FROM ("
                    f" SELECT measure, COUNT(*) c FROM {t} GROUP BY measure)"
                ).result()
            )
        )[0]
        dups = next(
            iter(
                client.query(
                    "SELECT COUNT(*) FROM ("
                    f" SELECT measure, datetime FROM {t} GROUP BY measure, datetime HAVING COUNT(*) > 1)"
                ).result()
            )
        )[0]

    print("== post-load summary ==")
    print(f"  rows={total:,} distinct_measures={distinct_m:,}")
    print(f"  per-measure coverage days: min={pmin} median={pmed} max={pmax}")
    print(f"  median rows/measure={cmed:,}")
    print(f"  duplicate (measure, datetime) keys={dups}")


def seed(
    destination: str,
    workers: int,
    duckdb_path: str,
    measure_limit: int | None,
    batch_size: int,
    dry_run: bool = False,
    max_requests: int | None = None,
    state_file: Path = DEFAULT_STATE_FILE,
    active_only: bool = False,
    active_within_days: int = 0,
    reset_state: bool = False,
    pace_seconds: float = 0.05,
) -> int:
    settings = get_settings()
    client = EAClient(settings)
    state = {"completed_measures": []} if reset_state else _load_state(state_file)
    done: set[str] = set(state["completed_measures"])
    print(f"state file: {state_file.resolve()} ({len(done)} completed)")

    measures = client.get_items("/id/measures")
    selected, stats = _select_measures(measures, measure_limit, active_only, active_within_days)
    todo = [m for m in selected if m not in done]
    if max_requests is not None:
        todo = todo[:max_requests]
    print(
        f"catalogue={stats['total']} selected={stats['selected']} "
        f"skipped_inactive={stats['skipped_inactive']} "
        + (f"reference={stats.get('reference')} " if stats.get("reference") else "")
        + f"already_done={len(done)} to_fetch={len(todo)}"
    )

    if dry_run:
        # 1 request per measure (the API returns each measure's full window in one
        # call). Runtime is dominated by network latency, not the paced sleep.
        lower = len(todo) * pace_seconds / max(workers, 1)
        est_day = stats["est_rows_day"]
        print(
            f"DRY RUN: {len(todo)} requests (workers={workers}); paced lower bound "
            f"{lower:.0f}s, realistic ~{len(todo) * 0.5 / max(workers, 1) / 60:.0f} min "
            "at ~0.5s/request."
        )
        print(
            f"  cadence-based estimate: ~{est_day:,} rows/day "
            f"(~{est_day * 7:,} for 7d, ~{est_day * 28:,} for ~28d). No data loaded."
        )
        client.close()
        return 0

    lock = threading.Lock()
    collected: list[dict] = []
    collected_measures: list[str] = []
    failures = 0
    loaded_measures = 0

    def flush() -> None:
        nonlocal loaded_measures
        if not collected:
            return
        result = load_readings_history(
            collected,
            settings=settings,
            destination=destination,
            duckdb_path=duckdb_path,
        )
        loaded_measures += len(collected_measures)
        done.update(collected_measures)
        print(
            f"  batch load: {result.rows_loaded:,} rows, "
            f"{result.rows_quarantined} quarantined ({loaded_measures} measures loaded)"
        )
        collected.clear()
        collected_measures.clear()
        _save_state(state_file, {"completed_measures": sorted(done)})

    def worker(uri: str) -> list[dict]:
        with EAClient(get_settings()) as thread_client:
            time.sleep(pace_seconds)  # gentle pacing
            return fetch_measure_readings(thread_client, uri)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(worker, uri): uri for uri in todo}
        for i, future in enumerate(as_completed(futures), start=1):
            uri = futures[future]
            rows = future.result()
            with lock:
                if rows:
                    collected.extend(rows)
                    collected_measures.append(uri)
                else:
                    done.add(uri)  # nothing to load; safe to checkpoint as done
                    failures += 1
                if len(collected) >= batch_size:
                    flush()
                if i % 25 == 0:
                    # Persist progress; pending (unflushed) rows are re-fetched on
                    # restart, so only `done` (loaded) measures are written here.
                    print(f"  progress: {i}/{len(todo)} measures")
                    _save_state(state_file, {"completed_measures": sorted(done)})

    flush()
    _save_state(state_file, {"completed_measures": sorted(done)})
    client.close()
    print(f"done: {len(done)} measures completed, {failures} empty/failed")
    summarize(destination, duckdb_path)
    return 0 if failures == 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--destination", choices=["bigquery", "duckdb"], default="bigquery")
    parser.add_argument("--duckdb-path", default="data/uk_flood.duckdb")
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument(
        "--measure-limit", type=int, default=None, help="Debug: only fetch N measures"
    )
    parser.add_argument("--batch-size", type=int, default=2000, help="Load every N measures")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Estimate request count/runtime only; load nothing.",
    )
    parser.add_argument(
        "--max-requests",
        type=int,
        default=None,
        help="Hard cap on measures fetched this run (budget guard).",
    )
    parser.add_argument(
        "--state-file",
        type=Path,
        default=DEFAULT_STATE_FILE,
        help="Resumable checkpoint file.",
    )
    parser.add_argument(
        "--active-only",
        action="store_true",
        help="Skip measures with no latestReading (verified to return 0 rows).",
    )
    parser.add_argument(
        "--active-within-days",
        type=int,
        default=0,
        help="Stricter freshness cutoff (days) relative to newest reading; 0 keeps "
        "stale-but-partial history.",
    )
    parser.add_argument(
        "--reset-state",
        action="store_true",
        help="Ignore an existing checkpoint (used for idempotency re-runs).",
    )
    parser.add_argument(
        "--pace-seconds",
        type=float,
        default=0.05,
        help="Per-request sleep per worker (be gentle to a beta API).",
    )
    args = parser.parse_args()
    return seed(
        destination=args.destination,
        workers=args.workers,
        duckdb_path=args.duckdb_path,
        measure_limit=args.measure_limit,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
        max_requests=args.max_requests,
        state_file=args.state_file,
        active_only=args.active_only,
        active_within_days=args.active_within_days,
        reset_state=args.reset_state,
        pace_seconds=args.pace_seconds,
    )


if __name__ == "__main__":
    raise SystemExit(main())
