"""Seed ~4 weeks of readings history from the live EA API.

The global /data/readings endpoint caps responses at 10 000 rows and rejects
``?since=`` (PRD §7 quirk 10), so history is pulled measure-by-measure
(``?measure=<uri>`` returns each measure's full ~4-week window in one call).

- concurrent with a small worker pool (be nice to a beta API)
- resumable: completed measures are appended to a state file
- every page is contract-validated and merged idempotently into the
  ``readings`` table, so re-running the script never creates duplicates

Usage:
    uv run python scripts/seed_history.py --destination duckdb
    uv run python scripts/seed_history.py --destination bigquery --workers 8
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from uk_flood.ea_client import EAClient, EARequestError
from uk_flood.pipelines import load_readings_history
from uk_flood.settings import get_settings

STATE_FILE = Path("data/seed_history_state.json")


def _load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    return {"completed_measures": []}


def _save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state), encoding="utf-8")


def fetch_measure_readings(
    client: EAClient, measure_uri: str, timeout_s: float = 30.0
) -> list[dict]:
    """One measure's readings; returns [] on deterministic client errors."""
    try:
        return client.get_items("/data/readings", {"measure": measure_uri, "count": "30000"})
    except EARequestError as exc:
        print(f"  WARN {measure_uri}: {exc}", file=sys.stderr)
        return []


def seed(
    destination: str,
    workers: int,
    duckdb_path: str,
    measure_limit: int | None,
    batch_size: int,
    dry_run: bool = False,
    max_requests: int | None = None,
) -> int:
    settings = get_settings()
    client = EAClient(settings)
    state = _load_state()
    done: set[str] = set(state["completed_measures"])
    print(f"state file: {STATE_FILE.resolve()} ({len(done)} completed)")

    measures = client.get_items("/id/measures")
    measure_uris = [m["@id"] for m in measures if "@id" in m]
    if measure_limit:
        measure_uris = measure_uris[:measure_limit]
    todo = [m for m in measure_uris if m not in done]
    if max_requests is not None:
        todo = todo[:max_requests]
    print(f"{len(measure_uris)} measures total, {len(done)} already done, {len(todo)} to fetch")

    if dry_run:
        # Estimate only: 1 request per measure (the API returns each measure's
        # full window in one call). Report the budget so the operator can decide
        # 7 vs 28 days before spending any quota.
        est_seconds = len(todo) * 0.05 / max(workers, 1)
        print(
            f"DRY RUN: {len(todo)} requests would be issued "
            f"(workers={workers}); ~{est_seconds:.0f}s paced lower bound. "
            f"No data loaded."
        )
        client.close()
        return 0


    lock = threading.Lock()
    collected: list[dict] = []
    failures = 0

    def worker(uri: str) -> list[dict]:
        # Each thread gets its own client (httpx.Client is not thread-safe)
        with EAClient(get_settings()) as thread_client:
            time.sleep(0.05)  # gentle pacing
            return fetch_measure_readings(thread_client, uri)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(worker, uri): uri for uri in todo}
        for i, future in enumerate(as_completed(futures), start=1):
            uri = futures[future]
            rows = future.result()
            with lock:
                collected.extend(rows)
                done.add(uri)
                if len(collected) >= batch_size:
                    result = load_readings_history(
                        collected,
                        settings=settings,
                        destination=destination,
                        duckdb_path=duckdb_path,
                    )
                    print(
                        f"  batch load: {result.rows_loaded} rows, {result.rows_quarantined} quarantined"
                    )
                    collected.clear()
                if i % 100 == 0:
                    _save_state({"completed_measures": sorted(done)})
                    print(f"  progress: {i}/{len(todo)} measures")
            if not rows:
                failures += 1

    if collected:
        result = load_readings_history(
            collected, settings=settings, destination=destination, duckdb_path=duckdb_path
        )
        print(f"  final load: {result.rows_loaded} rows, {result.rows_quarantined} quarantined")

    _save_state({"completed_measures": sorted(done)})
    client.close()
    print(f"done: {len(done)} measures completed, {failures} empty/failed")
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
    args = parser.parse_args()
    return seed(
        destination=args.destination,
        workers=args.workers,
        duckdb_path=args.duckdb_path,
        measure_limit=args.measure_limit,
        batch_size=args.batch_size,
        dry_run=args.dry_run,
        max_requests=args.max_requests,
    )


if __name__ == "__main__":
    raise SystemExit(main())
