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
- D-015: Executed 2026-11-14. The old `readings` table was backed up to GCS Parquet,
  renamed `readings_legacy`, and replaced by a deduplicated partitioned `readings`
  (22,853,023 rows; 0 mismatched days). Keep `readings_legacy` and
  `readings_archive_staging` until explicit cleanup approval. Scheduled dlt history
  writes are append-only; staging dedups raw and `fct_readings` reprocesses a 3-day
  lookback with `insert_overwrite`. The 2026-11-11..11-17 gap was filled from the
  local archive with a free load job (2,960,892 rows) and verified against per-day
  counts. Re-pull the local archive before ~2026-12-14 and weekly until Airflow owns
  ingestion. See `docs/DECISIONS.md`.
- D-016: BigQuery auth uses ADC impersonation, not a downloaded key. Point
  `GOOGLE_APPLICATION_CREDENTIALS` at an impersonated ADC JSON file; dlt and dbt both
  resolve it through ADC. Never commit the ADC file. See `docs/DECISIONS.md`.
- D-017: Cost-gated Airflow cadence. A full ingest cycle bills ~580 MiB, so the
  DAG ingests readings every 15 minutes, refreshes reference data daily at 02:00 UTC,
  and runs dbt every 2 hours. Gates read only the Airflow interval (0 query bytes);
  trips append a free ops row to `uk_flood_ops.gate_events`, optionally alert (off by
  default), and appear in `uk_flood_monitoring.freshness`. Both gates fail closed by
  default. A widened 7-day gap rebuild billed 4.45 GiB (worst case), so 2-hourly
  is not justified by that measurement; the steady-state 3-day run is pending.
  Set `DBT_INTERVAL_HOURS=4` if the total exceeds 15 GiB/day. Do not restore
  full-reference ingestion every 15 minutes without re-measuring.
  See `docs/DECISIONS.md`.
