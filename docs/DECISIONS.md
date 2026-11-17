# Decisions Log — UK Flood Pipeline

Recorded decisions, rationale, and status. Newest entries at the bottom of each
section. Every entry states the evidence it rests on.

---

## D-001: BigQuery location — migrated to europe-west2 (EXECUTED)

- **Status:** done (2026-10-26).
- **Constraint:** BigQuery dataset IDs are unique per project, so the same names
  cannot coexist in US and europe-west2.
- **Executed:** rehearsed in temporary `*_euw2` datasets (row counts, dbt build,
  Elementary all verified), then dropped the US datasets and recreated the canonical
  names in europe-west2; the temp datasets were removed after verification.
- **Evidence:** every canonical dataset reports `location=europe-west2`. Raw
  `readings` = 15,429,146 rows (2021-11-01 → 2026-10-26, 4.34 GB); `fct_readings` =
  14,249,498 rows (90-day partition expiry); 0 duplicate keys.

## D-002: Airflow 3 upgrade — DEFERRED

- **Status:** decided (defer).
- **Evidence:** official current release is **3.3.2** (checked on PyPI, 2026-10-22);
  repo pins **2.10.4**.
- **Rationale:** Airflow 2→3 is a major release with breaking DAG/API changes
  (`schedule_interval` → `schedule`, connection/permission model, provider split).
  The pipeline is working; a major upgrade risks silent breakage for little project
  value. **Decision: stay on the 2.x line and bump to the latest 2.x patch only**
  (verify the exact image/digest before pinning). Revisit after the observability
  and orchestration phases if there is a concrete driver.

## D-003: Elementary upgrade — compatibility check required, before baselines

- **Status:** checked locally 2026-10-27; still open before baselines.
- **Evidence:** the **Python CLI** in `.venv` is `elementary-data 0.26.1`; the
  **dbt package** is pinned `elementary-data/elementary [">=0.13.0","<0.17.0"]`
  (locked `0.16.4`). dbt-core is `1.9.11`. The dbt package declares
  `require-dbt-version: [">=1.0.0", "<2.0.0"]`; the CLI metadata requires
  `dbt-core >=1.8,<2.0.0` when a dbt extra is installed. Both accept dbt-core
  1.9.11.
- **Local finding:** the 0.26.1 CLI's entry point is `edr`, not `elementary`.
  The Makefile target was calling `uv run elementary generate-report`, which
  cannot spawn. It now calls `uv run edr report --profiles-dir dbt
  --project-dir dbt`. This is a command fix only; no warehouse was queried.
- **Unresolved:** CLI 0.26.1 and dbt package 0.16.4 are not the same
  generation. The dbt package README and models are still 0.16.4-era; a 0.26.x
  CLI report may not fully understand their tables. Version alignment is not
  proven.
- **Rationale/next:** align the pair **before** any baselines are established,
  so a package change cannot invalidate accumulated anomaly metric history.
  Prefer upgrading the dbt package to the 0.26.x line (bump `packages.yml` +
  `package-lock.yml`, `dbt deps`, `dbt parse`) or pin the CLI to the 0.16.x
  generation. Do this in an isolated commit; the actual upgrade needs a
  BigQuery-connected validation run, so it waits for the quota/enforcement
  gate.

## D-004: Elementary baselines are history-based, not single-run

- **Status:** decided (informational; governs ordering).
- **Evidence:** `get_anomaly_scores_query.sql` reads stored history from
  `elementary.data_monitoring_metrics` and computes rolling
  `training_avg`/`training_stddev` over `days_back` (default **14**) buckets keyed
  by the model's `timestamp_column` (`reading_time` on `fct_readings`).
- **Implication:** a baseline requires **≥ `days_back` days of stored metric
  history**, which only accumulates as monitor/test **runs** happen. Seeding
  historical *data* does not by itself create baseline history.
- **Ordering rule:** migrate → seed history → build core → run
  `dbt test -s tag:elementary` repeatedly until the training window is populated →
  only then claim baselines. Until then, anomaly results must be marked
  **unverified** (thin/empty training sets pass trivially).
- **Observed post-backfill (2026-10-26):** the first `dbt build --full-refresh`
  after the 15.4M-row seed fired an Elementary `column_anomalies` test on
  `fct_readings.reading_value` min/max (historical range −4,259.8 to ~2.0e7; null
  rate 0%). A second `dbt build` passed 85 PASS / 1 WARN / 0 ERROR. Rule: after any
  backfill or full-refresh, run dbt twice (or use `elementary backfill`) before
  treating an anomaly as a regression.

## D-005: Data-quality WARN on `stg_measures.unit_name` is expected

- **Status:** decided (keep warn).
- **Evidence:** the EA API returns units beyond its documented set: `mASD`,
  `mAOD`, `m3/s`, `mBDAT`, `Ml/d`, `deg C`, `V`, `m/s`, `deg`, `%`, `Knots`,
  `Knot`, `m2`, `mV`, `mACD` (live counts in `docs/data-dictionary.md`).
- **Decision:** keep `severity: warn`; document as a known API quirk. Extend the
  whitelist only for confirmed, stable units. Surface off-list distinct-count in
  the observability layer (Phase C).

## D-006: `run_metadata` write failures must fail loudly

- **Status:** decided (implemented Phase B).
- **Evidence:** BigQuery had **no** `run_metadata` table while DuckDB did; the
  writer swallowed exceptions with `logger.exception` only.
- **Decision:** `_write_run_metadata` now sets `report.status='failed'` and
  re-raises. Metadata loss = loss of the entire audit trail, so it is treated as a
  run failure.

## D-007: Historical seed endpoint and active-measure semantics

- **Status:** decided (implemented Phase B).
- **Evidence:** `/data/readings?measure=` returns 0 rows; `?count=` → HTTP 400;
  `/id/measures/{id}/readings` returns full history (2,850 rows / 29 days verified).
  Measures with `latestReading == null` return 0 rows; stale measures return partial
  history (763 rows / 19 days verified).
- **Decision:** seed via the per-measure endpoint; `--active-only` skips only
  null-latest measures and retains stale-but-partial ones. This fixes the root cause
  of `readings` containing only the latest snapshot.

## D-008: Raw archive is kept; fct retention is 90 days

- **Status:** decided (2026-10-26) — raw expiry NOT applied.
- **Evidence:** raw `readings` is unpartitioned and `dlt`'s BigQuery config exposes
  no partition-expiration option. After the full seed it holds **15,429,146 rows**
  (2021-11-01 → 2026-10-26, **4.34 GB**, ~$0.09/month).
- **Decision:** keep the full raw archive (explicit user choice, supersedes the
  earlier 90-day raw-expiry plan). `fct_readings` keeps
  `partition_expiration_days=90` (14,249,498 rows, 2026-07-28 → 2026-10-26) so the
  marts and query costs stay bounded.
- **Utility:** `scripts/apply_retention.py` remains available (dry-run first) if the
  archive ever needs trimming; it is not part of the normal run path.

## D-009: Every contract table gets explicit dlt column hints

- **Status:** decided (fix implemented).
- **Evidence:** a live reload into a fresh BigQuery dataset dropped all-NULL
  columns (`readings.qualifier`, `flood_warnings.situation`); dbt then failed with
  `Unrecognized name`. The same class of bug explains why `run_metadata` never
  existed in BigQuery.
- **Decision:** declare `data_type` for every contract field in `TABLE_COLUMNS`
  (nested `stations.measures` excluded — dlt unnests it). `tests/test_column_hints.py`
  asserts every contract field has a hint, so future contract additions cannot
  silently drop a column.

## D-010: Reset global dlt state when the destination dataset changes

- **Status:** decided.
- **Evidence:** after repointing dlt at the new `_euw2` dataset, dlt's restored
  schema said `run_metadata` existed and issued a load with `CREATE_NEVER`, which
  failed 404 because the table did not exist in the new dataset. dlt state lives in
  `~/.dlt/pipelines/uk_flood_raw`, not the repo.
- **Decision:** any dataset/location move must delete `~/.dlt/pipelines/uk_flood_raw`
  (and project-local `.dlt` if present) and reload; never reuse state across datasets.

## D-011: BigQuery daily query quota is 31,457 MiB, reset at midnight Pacific

- **Status:** confirmed by the user 2026-10-27; recorded, enforcement test still
  pending.
- **Decision:** treat **31,457 MiB = 30.72 GiB** as the BigQuery "Query usage per
  day" quota. The quota day resets at **midnight America/Los_Angeles**, not UTC:
  **07:00 London now, 08:00 London after 1 Nov**. This supersedes the earlier
  approximate "30 GiB/day" wording.
- **Do not confuse with:** `MAX_BYTES_BILLED=10737418240` (10 GiB), which is a
  per-query safety cap, not the daily quota.
- **Evidence:** the 2026-10-27 Phase C analysis day used **30.98 GiB** across
  **121 query jobs** (`europe-west2` 30.92 GiB + `US` 0.06 GiB), about **0.26 GiB
  above** the exact quota. No quota error was observed, and the export is
  complete. Do not run further BigQuery queries until the enforcement test is
  confirmed and the quota has reset.
- **Recording:** `.env.example` now carries
  `BQ_DAILY_QUERY_QUOTA_MIB=31457` and
  `BQ_QUOTA_RESET_TZ=America/Los_Angeles`.
- **Proposed daily allocations** (not measurements, not entitlements):

  | Workload | Allocation (MiB) | Share |
  |---|---:|---:|
  | Ingest/dlt query work | 4,096 | 13.0% |
  | Scheduled dbt models | 10,240 | 32.6% |
  | Scoped tests | 2,048 | 6.5% |
  | Development/analysis | 4,096 | 13.0% |
  | Reserve | 10,977 | 34.9% |
  | **Total** | **31,457** | **100%** |

  The weekly full-history suite is not covered by this table until it is
  re-measured after partitioning; it must use the reserve or the D-012
  fallback. If a workload exceeds its allocation, stop and re-plan rather than
  silently borrowing from reserve.

## D-012: Weekly full-history dbt tests are held as a fallback

- **Status:** held as fallback 2026-10-27; not implemented and not run.
- **Evidence:** daily tests use `test_lookback_days=3`; `make dbt-test-full`
  sets it to `100000` and runs the full archive. The earlier full-suite estimate
  was roughly **40 GB**, which exceeds the **30.72 GiB/day** quota before any
  other work. That estimate likely includes unpartitioned raw scans, so it is
  not a reliable post-migration cost.
- **Decision:** keep the daily 3-day scoped run. Do **not** implement or run the
  four-way split now. Re-measure the full suite after the partitioned migration;
  if it still cannot fit a quota day, implement the split only as a fallback.
- **Fallback:** split across four consecutive quota days, each starting after
  midnight Pacific: (1) staging, (2) core, (3) marts, (4)
  singular/Elementary/remaining. Each group is ~10 GB by the old estimate, so
  every test still runs once per week. Alternatively, rotate archive windows
  (for example one year per week over four weeks, or a 30-day weekly check plus
  a quarterly full-archive run split by year).
- **Gate:** do not run or implement these targets until the partitioned path is
  measured and the user confirms the enforcement test and quota position.
## D-013: settings.max_bytes_billed is inert for dlt; daily quota is the backstop

- **Status:** documented 2026-10-27.
- **Evidence:** dlt 1.31.0 has no maximum_bytes_billed/max_bytes_billed support in the BigQuery destination; dlt BigQuery loads use **load jobs**. The merge follow-up noted in Phase C was a query job without a max-bytes option. dbt still uses maximum_bytes_billed at the profile level (a different mechanism).
- **Decision:** Settings.max_bytes_billed is **inert** for dlt ingestion. The **BigQuery \"Query usage per day\" quota (31,457 MiB, reset midnight America/Los_Angeles / 07:00 London)** is the effective backstop for dlt/BigQuery. Do **not** rely on per-query byte caps in dlt code.
- **Verification (post-migration):** after the partitioned migration, verify via BigQuery job labels/history that dlt issues **load jobs only** (no ad-hoc query jobs) for ingestion paths; if any query jobs appear, they must be reviewed against the daily quota.

## D-014: Elementary pinned to 0.17.0, not 0.26.1

- **Status:** decided 2026-10-27; pin kept.
- **Evidence:** `dbt/packages.yml` pins `elementary-data/elementary` to `>=0.17.0,<0.18.0`; `dbt/package-lock.yml` and the installed `dbt/dbt_packages/elementary/dbt_project.yml` are 0.17.0; `uv.lock` pins `elementary-data` 0.17.0. The 0.26.1 dbt package accepts dbt-core 1.9.x (`require-dbt-version: >=1.0.0,<3.0.0`), so dbt-core is not the blocker.
- **Decision:** keep the package and CLI on the same validated 0.17.x generation rather than mixing in 0.26.1. The 0.18+/0.26.x generation changes Elementary models/macros/tables and requires a BigQuery-connected upgrade validation (dbt deps/parse/build + report); it could also invalidate the stored anomaly baselines (D-003/D-004). That validation is gated behind the quota/partitioned-migration work.
- **Revisit:** when the quota/partitioned-migration gate allows a connected upgrade run; bump the dbt package, CLI, and lockfiles together, then re-establish baselines.

## D-015: Preserve readings_legacy and refresh the local archive weekly

- **Status:** executed 2026-11-14; `readings_legacy` and
  `readings_archive_staging` retained pending explicit cleanup approval.
- **Context:** The EA API retains a rolling ~28-day window. The local archive spans
  2021-11-01..2026-11-11; the old BigQuery `readings` table spanned
  2021-11-01..2026-10-26. The archive and legacy windows overlap but are not identical;
  legacy-only keys (including 29 Sep-13 Oct) are preserved by legacy-first dedup.
- **Archive correction:** the first parquet build used DuckDB `TIMESTAMPTZ` parsing in
  the session timezone, which misread UTC `Z` timestamps across the spring DST boundary.
  That produced 140 false duplicate groups and shifted 140 rows. The build now reads
  `dateTime` as VARCHAR and sets `TimeZone='UTC'`; the corrected archive has
  **15,451,487 rows, 0 duplicate `(measure, datetime)` keys**, and a regression test
  covers the DST case. The earlier 84 identical / 56 conflicting report was an artifact.
- **Decision:** Never drop the old table. It was backed up to GCS Parquet, renamed to
  `readings_legacy`, and the new partitioned `readings` was built as a deduplicated
  union of `readings_legacy` and archive staging. Keep both `readings_legacy` and
  staging as the Phase F “before” baseline until explicit cleanup approval. Do not
  append the archive to the live table; build once via insert.
- **Drop conditions for the baseline:** only after all of these are true:
  1. Per-day counts match for the full legacy and archive ranges (already satisfied:
     1,805 days, 0 mismatched days).
  2. The GCS backup is verified restorable by reading the Parquet back through an
     external table and matching `readings_legacy` row count (15,429,835) and archive
     staging row count (15,451,487).
  3. Explicit cleanup approval is recorded. Until then, do not drop either table.
- **Execution evidence:**
  - GCS backup: private bucket in `europe-west2` (name intentionally not recorded in
    committed docs), prefix `backup/readings_legacy_2026-11-11_*.parquet`
    (147 objects, 467,635,474 B); archive parquet at prefix
    `archive/readings_2026-11-11.parquet` (77,720,696 B). Bucket has uniform
    bucket-level access, public access prevention enforced, and a 90-day delete
    lifecycle rule.
  - Restore check: external Parquet tables returned 15,429,835 legacy rows and
    15,451,487 archive rows; external tables were dropped after verification.
  - Staging: 15,451,487 rows, 3,637,963,985 logical bytes, 0 null required fields,
    0 duplicate keys.
  - Exact INSERT dry-run: **7,976,032,165 B** (`UPPER_BOUND`); legacy scan was
    4,338,068,180 B (`PRECISE`). Load jobs were free.
  - New `readings`: **22,853,023 rows**, 6,684,515,906 logical bytes,
    `PARTITION BY DATE(datetime)`, `CLUSTER BY measure`; 15,429,835 legacy rows +
    7,423,188 archive rows; 0 required nulls; 22,853,023 distinct `_dlt_id`s.
  - Reconciliation: 1,805 days, union daily counts 22,853,023, **0 mismatched days**;
    span 2021-11-01..2026-11-11 22:30 UTC.
  - Live appends since the swap: **22,858,406 rows** through 2026-11-16T17:10:53Z;
    the 11-12..11-16 gap is still sparse until the resume/backfill completes.
  - Swap: `readings` -> `readings_legacy`; `readings_new` -> `readings`.
- **Dedup rule:** legacy rows win the overlap (`source_priority=0`), archive fills
  missing keys and the tail (`source_priority=1`); keep the lexicographically smallest
  `id` within a source. Archive rows get a deterministic synthetic `_dlt_load_id`
  (archive snapshot) and `_dlt_id` (hash of measure/datetime/reading_id); preserve
  dlt's `measure`/`datetime`/`_dlt_load_id`/`_dlt_id` NOT NULL hints.
- **Freshness:** the local archive was refreshed 2026-11-17 00:08 UTC:
  **18,410,825 rows**, span 2021-11-01..2026-11-17T00:08:35Z, 5,386 measures,
  parquet 92,819,255 B, 0 duplicate `(measure, datetime)` groups. It now covers the
  11-12..11-17 gap and can be used as a fallback source. Re-pull before ~2026-12-15
  and weekly until the Airflow DAG owns ingestion. `scripts/archive_history.py` is
  incremental (`since` + 1-day overlap, merge by `@id`).
- **Post-swap:** scheduled dlt history dual-write is append-only. Staging dedups raw
  with `QUALIFY ROW_NUMBER()` by `(measure, datetime)` ordered by latest
  `_dlt_load_id`/`_dlt_id`. `fct_readings` uses BigQuery `insert_overwrite` with a
  3-day lookback (`fct_lookback_days`); widen that var for late/backfilled data.
  `load_readings_history` backfill remains merge-based for idempotent seeding.
- **Gap fill (2026-11-17):** the local archive was loaded into `readings` with a
  free BigQuery load job, not `seed_history`. The boundary was detected from
  `INFORMATION_SCHEMA.PARTITIONS` counts, not the sparse latest-reading max: the
  last complete day was 2026-11-10, so the load started at 2026-11-10 22:55 UTC
  (1-hour overlap) and ended at the archive max 2026-11-17 00:08:35 UTC. The
  filtered Parquet was 2,960,892 rows / 69,333,461 B; the load job appended all
  rows. Raw counts after load: 11-10 485,807; 11-11 513,688; 11-12 506,797;
  11-13 506,646; 11-14 506,487; 11-15 506,154; 11-16 416,425; 11-17 318. The
  archive itself has only 410,896 rows for 11-16 (83% of the 493,001 median), so
  raw matches the archive; no day is zero or below 80% of its archive count.
  Expected raw duplicates from the 1-hour overlap and the old sparse latest rows
  remain; staging dedups them. A widened 7-day `dbt build --select fct_readings`
  then reprocessed the gap; all data tests passed (one expected plausibility warn).
- **Cost:** legacy scan 4,338,068,180 B; staging 3,637,963,985 B; INSERT
  7,976,032,165 B. Load jobs are free; staging storage is ~3.64GB logical until
  cleanup. A measured `fct_readings` run with a 7-day lookback processed
  ~460 MB for the temp table and ~491 MB for the partition-scoped MERGE; the MERGE
  included `date(reading_date) in unnest(dbt_partitions_for_replacement)`. A 3-day
  lookback run without the staging filter processed 6.5 GB + 3.36 GB, which is why
  staging is now bounded before the window function.

## D-016: Use ADC impersonation, not a downloaded service-account key

- **Status:** configured 2026-11-16.
- **Decision:** Runtime BigQuery access uses Application Default Credentials (ADC).
  For impersonation, `GOOGLE_APPLICATION_CREDENTIALS` points at an impersonated ADC
  JSON file; no service-account key is committed or downloaded.
- **Setup:**
  - Grant the operator `roles/iam.serviceAccountTokenCreator` on the runtime SA
    (`dlt-runner@<GCP_PROJECT_ID>.iam.gserviceaccount.com`).
  - Enable `iamcredentials.googleapis.com`.
  - Create an impersonated ADC file with type `impersonated_service_account` and
    `service_account_impersonation_url` pointing at the runtime SA; copy the existing
    `authorized_user` fields into `source_credentials`. Keep this file outside the repo.
  - Set `GOOGLE_APPLICATION_CREDENTIALS` to that file and `GOOGLE_CLOUD_PROJECT` to the
    project id.
- **Why:** dlt does not accept an impersonated credentials object through its
  `credentials=` argument, but it does use impersonated ADC when `credentials` is not
  passed explicitly. `make_pipeline` therefore passes `project_id` and lets ADC resolve.
  dbt also uses ADC because `keyfile` was removed from the profile.
- **Verification:** `gcloud auth print-access-token --impersonate-service-account=...`
  succeeds; a dlt probe load into a temporary dataset succeeded through the
  impersonated ADC file; `dbt debug --target dev` connected with `method: oauth`.
- **Guardrail:** never commit the impersonated ADC file or any refresh token. The
  standard `%APPDATA%\gcloud\application_default_credentials.json` is also outside the
  repo.

## D-017: Cost-gated Airflow cadence (readings 15 min, reference daily, dbt configurable)

- **Status:** decided and implemented 2026-11-16; budget measurement pending the
  2026-11-17 quota reset.
- **Evidence:** one full ingestion cycle (stations, measures, flood_warnings,
  readings_latest) billed **608,174,080 B (~580 MiB)**. Breakdown by destination:
  stations 52.4 MB, stations__measures 52.4 MB, measures 41.9 MB, flood_warnings
  41.9 MB, readings_latest 41.9 MB, and ~262 MB of dlt script overhead. At a
  15-minute cadence that is ~55.7 GiB/day for full ingestion alone, before dbt.
- **Quota:** the project hit the **31,457 MiB/day** QueryUsagePerDay quota on
  2026-11-16 after a measured 9.9 GB dbt run, the 608 MB ingest, and diagnostic
  queries (32.37 GB billed). The quota resets at midnight America/Los_Angeles
  (08:00 UTC in November). The SA lacks `bigquery.jobs.listAll`, so the DAG cannot
  read project-wide `INFORMATION_SCHEMA.JOBS`; cost evidence is collected out of
  band with `JOBS_BY_USER`.
- **Decision:** keep the DAG on `*/15 * * * *`, but gate the expensive work:
  - every 15 minutes: ingest `readings_latest` only (dual-writes `readings`);
  - daily at 02:00 UTC: ingest `stations`, `measures`, `flood_warnings`;
  - every `DBT_INTERVAL_HOURS` at minute 0 (default 2; set 4 if the measured
    budget is exceeded): run `dbt build`, then Elementary.
- **What the gates read / cost:** `should_ingest_reference` and
  `should_build_dbt` read only the Airflow `data_interval_end` (fallback
  `logical_date`). They do **not** query BigQuery, so a gate costs **0 query
  bytes**. The optional ops row is a BigQuery streaming insert (also free); it
  does not consume QueryUsagePerDay. This is deliberate: a cost gate that queried
  `INFORMATION_SCHEMA.JOBS` would itself spend quota and would fail when the
  project is already over quota.
- **Trip behaviour:** `evaluate_gate` records every decision in
  `uk_flood_ops.gate_events` (gate, decision, reason, interval_end, fail_mode,
  alert_sent). A trip (`decision=false`) also calls the optional webhook in
  `OPS_ALERT_WEBHOOK_URL`; alerting is **off by default**. The dbt model
  `uk_flood_monitoring.freshness` exposes `gate_trips_24h`,
  `last_gate_trip_at`, `last_gate_trip_reason`, `gate_alerts_24h`, alongside
  latest/latest-history/fct lag minutes. If the ops insert or webhook fails, the
  failure is logged and the gate decision is unchanged.
- **Fail-closed vs fail-open:** both gates default to **closed** on an evaluation
  exception or missing interval: skip the guarded task and record
  `gate_error_closed:*`. Set `OPS_GATE_FAIL_MODE=open` to run the task instead
  (recorded as `gate_error_open:*`). Closed is the safer default for the quota;
  open is appropriate only if freshness matters more than the extra bytes.
- **Measured widened-lookback rebuild (2026-11-17 01:21 UTC):**
  `dbt build --select fct_readings --vars '{"fct_lookback_days": 7}'` billed
  **4,779,409,408 B (~4.45 GiB)** across 44 billed query jobs: fct CTAS 1.20 GB, MERGE
  1.20 GB, generated script 1.23 GB, Elementary tests ~0.8 GB, dbt metadata
  ~0.35 GB. This filled the 11-11..11-17 gap; it is a worst-case rebuild, not
  steady state. A follow-up 3-day run hit the same custom quota error, so the
  steady-state 3-day number is still pending.
- **Budget (15 GiB/day rule):**
  - readings-only ingest: 41.9 MB/run x 96 = **4.02 GB/day** (before dlt overhead);
  - reference refresh: **0.26 GB/day**;
  - dbt 2-hourly at 951 MB/run = **11.41 GB/day**;
  - subtotal **15.69 GB/day (~14.6 GiB)** before dlt overhead; at the measured
    4.45 GiB gap rebuild, 2-hourly would be **53.4 GiB/day**.
  - **Conclusion:** 2-hourly is not justified by the gap-rebuild measurement.
    The steady-state 3-day run must be measured after the quota resets; if it is
    above ~800 MB/run (or total query bytes exceed 15 GiB/day), set
    `DBT_INTERVAL_HOURS=4` (6 runs/day) or tighten `fct_lookback_days`. Do not
    restore full-reference ingestion every 15 minutes without re-measuring.
- **Gate:** after the quota resets, measure a readings-only ingest and a
  steady-state 3-day dbt run from `INFORMATION_SCHEMA.JOBS_BY_USER`; record the
  bytes in this decision. If the total exceeds the 15 GiB/day rule, use
  `DBT_INTERVAL_HOURS=4` or tighten `fct_lookback_days`.
- **Implementation:** `dags/uk_flood_pipeline.py` uses `ShortCircuitOperator`
  gates (`should_ingest_reference`, `should_build_dbt`) wrapped by
  `uk_flood.ops.evaluate_gate`, and the `--resources` CLI subset; the dbt gate
  waits for the daily reference load when it runs. `DBT_INTERVAL_HOURS` (default 2)
  controls the dbt cadence; set it to 4 if the measured daily total exceeds the
  15 GiB rule.
