"""CLI entrypoint: ``python -m uk_flood.ingest`` (also wired to ``make ingest``)."""

from __future__ import annotations

import json
import sys

import typer

from uk_flood.ea_client import EAClient
from uk_flood.pipelines import run_ingestion
from uk_flood.settings import get_settings

app = typer.Typer(add_completion=False, help="EA Flood Monitoring ingestion CLI")


@app.command()
def run(
    destination: str = typer.Option("bigquery", "--destination", "-d", help="bigquery | duckdb"),
    duckdb_path: str = typer.Option("data/uk_flood.duckdb", "--duckdb-path"),
    base_url: str = typer.Option("", "--base-url", help="Override EA API base URL (e.g. mock API)"),
    resources: str = typer.Option(
        "",
        "--resources",
        help="Comma-separated subset: stations,measures,flood_warnings,readings_latest",
    ),
    limit_stations: int = typer.Option(0, "--limit-stations", help="Debug: cap rows (0 = no cap)"),
    json_out: bool = typer.Option(False, "--json", help="Print the run report as JSON"),
) -> None:
    """Run one full ingestion cycle (4 resources + quarantine + run_metadata)."""
    settings = get_settings()
    if base_url:
        settings = type(settings)(**{**settings.__dict__, "ea_api_base_url": base_url.rstrip("/")})
    client = EAClient(settings)
    selected = [r.strip() for r in resources.split(",") if r.strip()] or None
    limits = {"stations": limit_stations} if limit_stations else {}

    report = run_ingestion(
        settings=settings,
        client=client,
        destination=destination,
        duckdb_path=duckdb_path,
        resources=selected,
        limits=limits,
    )

    if json_out:
        typer.echo(
            json.dumps(
                {
                    "run_id": report.run_id,
                    "load_id": report.load_id,
                    "status": report.status,
                    "total_loaded": report.total_loaded,
                    "total_quarantined": report.total_quarantined,
                    "resources": [vars(r) for r in report.resources],
                },
                default=str,
                indent=2,
            )
        )
    else:
        typer.echo(f"run_id={report.run_id} status={report.status}")
        for r in report.resources:
            flag = "schema-change!" if r.schema_change else ""
            typer.echo(
                f"  {r.resource:<18} loaded={r.rows_loaded:<6} quarantined={r.rows_quarantined:<4} "
                f"{r.duration_s:6.2f}s {r.status} {flag}"
            )
            if r.error:
                typer.echo(f"    error: {r.error}")
        typer.echo(f"total loaded={report.total_loaded} quarantined={report.total_quarantined}")
        if report.status == "failed":
            sys.exit(1)


@app.command()
def resources() -> None:
    """List the ingestion resources this pipeline manages."""
    from uk_flood.pipelines import RESOURCE_SPECS

    for name, spec in RESOURCE_SPECS.items():
        typer.echo(f"{name:<18} {spec['path']:<22} -> {spec['table']}")


if __name__ == "__main__":
    app()
