"""One-shot local pipeline: mock EA API -> duckdb raw tables.

Used by `make ingest-local` to produce a local warehouse for dbt iteration
(`make dbt-build-local`) without touching BigQuery or the live API.
"""

from __future__ import annotations

import argparse
from dataclasses import replace
from pathlib import Path

from mock_api.app import FaultConfig
from mock_api.server import MockServer
from uk_flood.ea_client import EAClient
from uk_flood.pipelines import run_ingestion
from uk_flood.settings import get_settings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb-path", default="data/uk_flood.duckdb")
    args = parser.parse_args()

    Path(args.duckdb_path).parent.mkdir(parents=True, exist_ok=True)
    server = MockServer(FaultConfig())
    url = server.start()
    try:
        settings = replace(get_settings(), ea_api_base_url=url)
        with EAClient(settings) as client:
            report = run_ingestion(
                settings=settings,
                client=client,
                destination="duckdb",
                duckdb_path=args.duckdb_path,
            )
        for r in report.resources:
            print(
                f"  {r.resource:<18} loaded={r.rows_loaded:<6} "
                f"quarantined={r.rows_quarantined:<4} {r.status}"
            )
        print(f"total loaded={report.total_loaded} quarantined={report.total_quarantined}")
        return 0 if report.status in ("success", "partial") else 1
    finally:
        server.stop()


if __name__ == "__main__":
    raise SystemExit(main())
