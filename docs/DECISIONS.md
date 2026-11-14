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
  staging until explicit cleanup approval. Do not append the archive to the live table;
  build once via insert.
- **Execution evidence:**
  - GCS backup: `gs://ukfloodpipeline-euw2-temp-20261111/backup/readings_legacy_2026-11-11_*.parquet`
    (147 objects, 467,635,474 B); archive parquet at
    `gs://ukfloodpipeline-euw2-temp-20261111/archive/readings_2026-11-11.parquet`
    (77,720,696 B).
  - Staging: 15,451,487 rows, 3,637,963,985 logical bytes, 0 null required fields,
    0 duplicate keys.
  - Exact INSERT dry-run: **7,976,032,165 B** (`UPPER_BOUND`); legacy scan was
    4,338,068,180 B (`PRECISE`). Load jobs were free.
  - New `readings`: **22,853,023 rows**, 6,684,515,906 logical bytes,
    `PARTITION BY DATE(datetime)`, `CLUSTER BY measure`; 15,429,835 legacy rows +
    7,423,188 archive rows; 0 required nulls; 22,853,023 distinct `_dlt_id`s.
  - Reconciliation: 1,805 days, union daily counts 22,853,023, **0 mismatched days**;
    span 2021-11-01..2026-11-11 22:30 UTC.
  - Swap: `readings` -> `readings_legacy`; `readings_new` -> `readings`.
- **Dedup rule:** legacy rows win the overlap (`source_priority=0`), archive fills
  missing keys and the tail (`source_priority=1`); keep the lexicographically smallest
  `id` within a source. Archive rows get a deterministic synthetic `_dlt_load_id`
  (archive snapshot) and `_dlt_id` (hash of measure/datetime/reading_id); preserve
  dlt's `measure`/`datetime`/`_dlt_load_id`/`_dlt_id` NOT NULL hints.
- **Freshness:** the archive was last refreshed 2026-11-11; report generated
  2026-11-14T02:58:39Z. Re-pull before ~2026-12-09 and weekly until the Airflow DAG
  owns ingestion. `scripts/archive_history.py` is incremental (`since` + 1-day overlap,
  merge by `@id`).
- **Post-swap:** scheduled dlt history dual-write is append-only; `fct_readings`
  reprocesses a 3-day lookback and merges on `(measure_id, reading_time)`.
  `load_readings_history` backfill remains merge-based for idempotent seeding.
- **Cost:** legacy scan 4,338,068,180 B; staging 3,637,963,985 B; INSERT
  7,976,032,165 B. Load jobs are free; staging storage is ~3.64GB logical until
  cleanup.
