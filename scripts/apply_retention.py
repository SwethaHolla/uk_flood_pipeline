"""Apply 90-day partition expiry to raw/staging reading tables.

Rationale: dlt creates BigQuery tables **unpartitioned** and exposes no
partition-expiration option, so retention cannot be configured through dlt. This
one-off/idempotent utility makes the raw reading table partitioned by date and
sets ``partition_expiration_days``.

- If the table is already partitioned on the target field, only the expiry option
  is set (no data movement).
- If it is not partitioned, the table is rebuilt partitioned (a single copy) via a
  staging table, then renamed back into place.
- ``--dry-run`` reports the current state and the SQL it *would* run.

This mutates BigQuery; it is invoked as part of the location migration, never by
tests or CI.
"""

from __future__ import annotations

import argparse

from google.cloud import bigquery

from uk_flood.settings import get_settings

DEFAULT_TABLES = [
    # (dataset, table, partition field expression, expiry days)
    ("{raw}", "readings", "datetime", 90),
]
EXPIRY_DAYS = 90


def _current_partitioning(table: bigquery.Table) -> str | None:
    tp = table.time_partitioning
    return tp.field if tp else None


def apply(dataset: str, table_name: str, field: str, days: int, dry_run: bool) -> None:
    settings = get_settings()
    client = bigquery.Client(project=settings.gcp_project_id)
    fq = f"`{settings.gcp_project_id}.{dataset}.{table_name}`"
    table = client.get_table(f"{settings.gcp_project_id}.{dataset}.{table_name}")
    field_expr = f"DATE({field})"
    partition_field = _current_partitioning(table)

    if partition_field:
        print(f"{fq}: already partitioned on {partition_field}")
        if dry_run:
            print(f"  would run: ALTER TABLE {fq} SET OPTIONS(partition_expiration_days={days})")
            return
        client.query(f"ALTER TABLE {fq} SET OPTIONS(partition_expiration_days={days})").result()
        print(f"  set partition_expiration_days={days}")
        return

    tmp = f"{table_name}__partitioned_tmp"
    sql = (
        f"CREATE OR REPLACE TABLE "
        f"`{settings.gcp_project_id}.{dataset}.{tmp}` "
        f"PARTITION BY {field_expr} "
        f"OPTIONS(partition_expiration_days={days}) AS SELECT * FROM {fq}"
    )
    swap = [
        sql,
        f"DROP TABLE {fq}",
        f"ALTER TABLE `{settings.gcp_project_id}.{dataset}.{tmp}` RENAME TO {table_name}",
    ]
    if dry_run:
        print(f"{fq}: NOT partitioned; would rebuild partitioned by {field_expr}")
        for s in swap:
            print("  " + s)
        return
    for s in swap:
        print(f"  running: {s[:100]}...")
        client.query(s).result()
    print(f"  rebuilt partitioned by {field_expr}, expiry={days}d")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    settings = get_settings()
    parser.add_argument("--dataset", default=settings.bq_raw_dataset)
    parser.add_argument("--table", default="readings")
    parser.add_argument("--field", default="datetime")
    parser.add_argument("--days", type=int, default=EXPIRY_DAYS)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    apply(args.dataset, args.table, args.field, args.days, args.dry_run)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
