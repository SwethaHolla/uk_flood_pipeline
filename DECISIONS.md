# Decisions

Canonical decision log: `docs/DECISIONS.md`. IDs here match that log.

- D-001: BigQuery location - migrated to europe-west2 (EXECUTED). See `docs/DECISIONS.md`.
- D-013: `settings.max_bytes_billed` is inert for dlt; the BigQuery daily query quota
  (31,457 MiB, reset midnight America/Los_Angeles) is the backstop. Load-job labels and
  backfill verification were added; confirm per label (load_job_ids/gcs_path/dir) on
  merge/verification. See `docs/DECISIONS.md`.
- D-014: Elementary pinned to 0.17.0, not 0.26.1. Why: `dbt/packages.yml` pins the dbt
  package to the validated 0.17.x generation (`>=0.17.0,<0.18.0`), `dbt/package-lock.yml`
  and `uv.lock` pin the matching 0.17.0 package/CLI, and the 0.18+/0.26.x generation
  changes Elementary models/macros/tables that need a BigQuery-connected upgrade
  validation (dbt deps/parse/build + report) and could invalidate stored anomaly
  baselines (D-003/D-004). 0.26.1 is not blocked by dbt-core (its dbt_project accepts
  dbt-core 1.9.x); it is deferred until the quota/partitioned-migration gate permits a
  connected validation run. Revisit then.
- D-015: Preserve the current BigQuery `readings` table as `readings_legacy` and never
  drop it until per-day counts in the new partitioned table match the whole legacy and
  archive ranges. Extract a Parquet backup first. Re-pull the local archive before
  ~2026-12-09 and weekly until Airflow owns ingestion. See `docs/DECISIONS.md`.
