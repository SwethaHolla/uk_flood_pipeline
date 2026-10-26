# Phase B Plan — Consistency, safety, data integrity, and the fct_readings investigation

**Status:** proposed, awaiting GATE approval. No dataset recreation performed.
**Date:** 2026-10-22
**Branch:** `phase-b-consistency` (to be created off `phase-a-audit`)
**Environment:** project `ukfloodpipeline`, BigQuery location **US**.

---

## 1. fct_readings row-count investigation (item 2) — root causes found

The audit question was "why only 5,404 rows?". Evidence gathered this session:

### 1.1 What the numbers say
```
readings:          n=5404  distinct(measure)=5404  distinct(datetime)=211
readings_latest:   n=5404  distinct(id)=5404       distinct(measure)=5404
fct_readings:      n=5404  distinct(reading_id)=5404  distinct(measure_id)=5404
fct_readings days: 22 distinct reading_date; range 2026-09-22 → 2026-10-21
```

`distinct(measure) == n` on **every** table is the smoking gun: **each measure has
exactly one reading row**. `readings` therefore contains only the latest snapshot,
not history. 211 distinct `datetime` values across 5404 rows just reflects many
measures sharing recent timestamps.

### 1.2 Root cause 1 — seed_history never loaded history into `readings`
- `data/seed_history_state.json` **does not exist** (verified) → `seed()` never
  completed even one checkpoint of 100 measures, and no `--measure-limit` smoke run
  left state.
- `readings` and `readings_latest` have identical row counts → `readings` was
  populated **only** by the `readings_latest` dual-write in `run_ingestion`
  (`pipelines.py:259-270`), which merges on `(measure, datetime)`.
- Conclusion: `seed_history.py` has **never been run against BigQuery**. The 22-day
  span is just the range of "latest" timestamps that the periodic ingestion happened
  to capture over ~1 month of runs — not backfilled history.

### 1.3 Root cause 2 — `run_metadata` is missing entirely on BigQuery
- `uk_flood_raw` contains `_dlt_loads, _dlt_pipeline_state, _dlt_version,
  flood_warnings, measures, quarantine, readings, readings_latest,
  stations, stations__measures` — **no `run_metadata`**.
- `_write_run_metadata` (`pipelines.py:378`) is wrapped in a broad
  `try/except` that only **logs** (`pipelines.py:422-423`). So the write can fail
  silently on BigQuery and nobody notices.
- The `.venv`-local DuckDB (`data/uk_flood.duckdb`, 6.5 MB) *does* have
  run_metadata (the offline path worked), proving the code logic is right and the
  BigQuery path failed supressed.
- One `_dlt_loads` row exists → the whole live dataset came from ~1 load (the dual
  write), consistent with 1.2.
- **Net effect:** we have **zero** recorded pipeline-run history in BigQuery, which
  is why there is no ops/SLI foundation either. This is the biggest integrity gap.

### 1.4 Fixes (Phase B scope)
1. Make `_write_run_metadata` failure **loud and non-silent**: on BigQuery, raise
   (or set report.status="failed") if metadata cannot be written, and add a test.
2. Fix `seed_history.py`:
   - state file path is `data/seed_history_state.json` **and is gitignored/absent**
     — make the state dir explicit and log its location; add a `--dry-run` that
     reports estimated requests/runtime **without** loading.
   - add a hard `--max-requests`/time budget so a 7-day smoke run cannot runaway.
3. After the europe-west2 migration (see §2), run a **7-day** seed first. Estimated
   cost reported before execution; extend to 28 days **only if approved** (item 2).

### 1.5 New tests (item 2)
- `test_date_span`: seeded/core data must span the requested window (>= N days).
- `test_min_rows_per_day`: each day in the window must have >= 1 row (or a documented
  floor) — guards silent partial loads.
- `test_zero_duplicates_on_rerun`: re-running the same load twice yields identical
  row counts and no duplicate `(measure, datetime)` / `reading_id`.

---

## 2. BigQuery location GATE (item 6 / irreversible)

- Current datasets are **US** (multi-region). Recorded decision: **europe-west2**.
- Dataset location is immutable → recreate datasets in europe-west2 + reload.
- Data today is tiny (≤7.5k rows/dataset, ~3.3 MB total) → **cheap** to redo.
- **This is the GATE.** No dataset will be created, dropped, or moved until the user
  approves §6 below.

### 2.1 Seed cost estimate (dry-run evidence, 2026-10-22)
Command (loads nothing):
```
uv run python scripts/seed_history.py --destination bigquery --dry-run
```
Output:
```
state file: ...\data\seed_history_state.json (0 completed)
7437 measures total, 0 already done, 7437 to fetch
DRY RUN: 7437 requests would be issued (workers=4); ~93s paced lower bound. No data loaded.
```
**Important honesty note:** the seeder issues **~7,437 requests either way** — the
global history endpoint caps at 10k rows and rejects `?since=`, so history is pulled
**per measure**, and each `?measure=<uri>` call returns that measure's **full ~4-week
window in one response**. There is currently **no server-side 7-vs-28-day filter**;
a "7-day seed" would still fetch the full window and then either load all of it or
filter client-side.

Estimated rows (15-min cadence ≈ 96/day/measure):
- 7-day window: ~**5.0 M** rows
- 28-day window: ~**20.0 M** rows

So the request/time cost is the same; the decision that matters for load size and
storage is 7-day (~5M rows, ~1.5 GB raw uncompressed order-of-magnitude) vs 28-day
(~20M rows). Both are within the free 1 TiB/month query tier for scans, but this is
**not** a "trivial" load and I will not understate it. Recommend: seed **7 days**,
measure actual bytes/time, then decide on 28.

---

## 3. Elementary baselines (item 3) — answered

Evidence from `dbt/dbt_packages/elementary/macros/edr/...`:
- Anomaly tests do **not** baseline off a single run. They read **stored history**
  from `elementary.data_monitoring_metrics` (see `get_anomaly_scores_query.sql`
  lines 49-102) and compute a rolling `training_avg` / `training_stddev` over a
  trailing window (`rows between unbounded preceding and current row`, lines
  166-170).
- The window is bounded by **`days_back`**, whose default is **14 days**
  (`get_config_var.sql:22`).
- Buckets are built from the model's configured **`timestamp_column`**
  (`reading_time` for `fct_readings`, `last_reading_at` for the marts model). So the
  baseline is time-indexed by the data's own timestamps, not by wall-clock run time —
  **but** the history rows only accumulate **when the tests actually run**, because
  each run writes new metric rows into `data_monitoring_metrics`.

### What this means for ordering
1. Baselines need **at least `days_back` (14) days of stored metric history** to be
   statistically meaningful (and ideally more so training_stddev is stable).
2. You cannot jump straight to a 28-day seed and then instantly have 28 days of
   baseline: the *metrics* are collected per test run. Historical *data* (seed) gives
   the timestamp range, but the anomaly engine still needs **repeated monitor runs**
   to populate `data_monitoring_metrics`.
3. Therefore the correct order is:
   - (a) migrate datasets → (b) seed history (7 days, then maybe 28) → (c) build core
     → (d) run `dbt test -s tag:elementary` on a schedule/backfill so metrics
     accumulate → (e) only then claim "baselines".
   - Until (d) has ≥ `days_back` of runs, anomaly tests will tend to pass trivially
     (thin/zero training set) and **must not** be cited as "passed".
4. Decision: set an explicit `elementary.days_back` matching the seed window, document
   the training-window requirement, and mark any "passed" claim as unverified until
   the window is populated.

---

## 4. Version upgrades (item 4)

- **Airflow:** current official is **3.3.2**; repo pins `2.10.4`. A 2→3 upgrade is a
  major, breaking change (DAG API, `schedule_interval`→`schedule`, connections).
  **Decision: DEFER.** Rationale recorded in `docs/DECISIONS.md`. Restrict to latest
  **2.x patch** (`apache/airflow:2.10.5-python3.12` if released, else 2.10.4) — verify
  the exact digest before pinning.
- **Elementary:** the **python** package is already 0.26.1 in `.venv`, but the **dbt
  package** is pinned `<0.17.0` (locked 0.16.4). Upgrade only **after** a dbt-core
  compatibility check (Elementary 0.17+/latest requires certain dbt-core ranges), and
  **before** baselines are established, so we don't invalidate metric history later.
  Plan: check compatibility matrix, bump `packages.yml` + `package-lock.yml` in a
  separate commit, re-run `dbt deps`/`dbt parse`, then document.

---

## 5. Known API quirk — 15 off-list units (item 5)

- `stg_measures.unit_name` accepted_values WARN fires on **15** rows with units
  outside `[m, Cumecs, mm, °C, m3/s, l/s, nt, knots]`.
- Action: keep `severity: warn`; **document as a known API quirk** in the data
  dictionary with the observed off-list values (captured at build time), plus a note
  that the whitelist will be extended only when a unit is confirmed real.

---

## 6. Service-account key removal (item 6) — gated, irreversible

- Today: `ukfloodpipeline-key.json` (untracked, gitignored) drives auth. Verified **not**
  in git history.
- Plan (only after SA-impersonation via ADC/user creds is proven working):
  1. Prove impersonation: `gcloud auth application-default login` + impersonate
     `dlt-runner@...`; run a real ingest + dbt build.
  2. Switch `settings.py`/`profiles.yml`/DAG to impersonation or WIF (no keyfile).
  3. **GATE:** propose deleting the local key file and (optionally) disabling the SA
     key in GCP inside a separate, explicit commit — **irreversible**, so it waits for
     a second approval.
- Do **not** delete the key in Phase B.

---

## 7. Additional Phase B corrections (item 1)
- **Chaos report bug:** connection-refused must be recorded as **failure**, not
  "[OK] Handled". Fix `scripts/chaos.py` reporting.
- **Pricing:** replace `$5/TB` with official **$6.25/TiB** (first 1 TiB/month free,
  10 MB minimum per table+query).
- **README:** mark chaos + benchmark sections **UNVERIFIED** until regenerated in
  Phases E/F.
- **`.env.example`:** remove `GOOGLE_APPLICATION_CREDENTIALS` key-file guidance;
  document impersonation/WIF, 30 GiB/day quota, £5 budget.

---

## 8. GATE — what I will NOT do until you approve
- Create/drop/move any BigQuery dataset (location migration).
- Seed history or run any paid load.
- Delete the SA key or disable it in GCP.
- Upgrade Airflow or Elementary major versions.

## 9. Proposed execution order after approval
1. Create branch `phase-b-consistency`; commit non-destructive fixes
   (§1.4, §3 decisions, §5 docs, §7 corrections) — **no cloud mutations**.
2. **GATE checkpoint #1:** show the europe-west2 migration + 7-day seed cost/runtime
   estimate. Wait.
3. On approval: migrate datasets, reload, seed 7 days, run tests (§1.5), build core,
   populate elementary window.
4. Report; **GATE checkpoint #2** for key deletion.

---

## 10. Pre-flight findings (2026-10-25) — supersedes earlier seed assumptions

### 10.1 Seeder was broken twice (root cause of zero history)
- `/data/readings?measure=<uri>` returns **0 rows** (it does not support measure filtering).
- `/data/readings?...&count=30000` returns **HTTP 400** ("Did not recognize request parameters [count]").
- Correct route: **`/id/measures/{id}/readings`** → returns the measure''s full available
  history (verified: 2,850 rows / 29 days for a 15-min measure).
- Fix applied in `fetch_measure_readings`.

### 10.2 Active-measure filter (empirically justified)
- EA measures have **no `status` field**. "Active" is inferred from `latestReading`.
- Verified by sampling (`/id/measures/{id}/readings`):
  - `latestReading == null` → **0 rows** (safe to skip; 2,064 measures).
  - stale `latestReading` → **partial history still returned** (e.g. 763 rows / 19d).
- Therefore `--active-only` keeps stale-but-present measures (5,373) and skips only
  null ones. `--active-within-days > 0` is the stricter option.

### 10.3 Corrected volume estimate (cadence-based, active measures)
```
uv run python scripts/seed_history.py --destination bigquery --active-only --dry-run
catalogue=7437 selected=5373 skipped_inactive=2064
cadence estimate ~482,712 rows/day  (~3,378,984 / 7d, ~13,515,936 / ~28d)
```
Pilot (100 active measures) scaled linearly ⇒ **~17.3M** because a small tail of
measures carries **much more than 4 weeks** (up to 893 days). Treat 13.5M as the
cadence floor and ~17M as the realistic total; the endpoint returns *full available
history*, so "no client-side filter" means loading that tail too.

### 10.4 Pilot evidence (100 active measures → local DuckDB)
```
rows=322,111  distinct_measures=100
per-measure coverage days: min=29 median=29 max=893
duplicate (measure, datetime) keys=0     (after identical re-run with --reset-state)
```
Median measure ≈ 29 days / ≈2,850 rows; a few long-archive measures dominate the tail.

### 10.5 Retention
- `fct_readings`: added `partition_expiration_days=90` (dbt-bigquery supports it).
- Raw `readings`: dlt creates it **unpartitioned** and exposes no expiry option, so
  `scripts/apply_retention.py` rebuilds it partitioned by `DATE(datetime)` with 90-day
  expiry (dry-run supported). Staging `stg_readings` is a view (N/A).

### 10.6 Migration naming constraint (needs decision)
BigQuery dataset IDs are **unique per project** — `uk_flood_raw` cannot exist in both
US and europe-west2. To honour "verify before dropping US", the plan is:
1. Create **temporary** europe-west2 datasets (`*_euw2`), load latest only, verify
   row counts + dbt build + Elementary against them; show results.
2. On approval, drop the US datasets, create the **canonical** names in europe-west2,
   then run the real load + full seed **once**.
This double-loads only the tiny latest snapshot, never the seed.

### 10.7 Runtime & rate-limit handling (measured 2026-10-25)
- Fetch throughput (DuckDB, live API): 40 measures → **47.1s @8 workers**, **37.8s @16 workers**
  ⇒ 5,373 measures ≈ **106 min @8w**, **85 min @16w** (API-latency bound).
- BigQuery merge/load overhead is additional; expect **~1.5–2 h** end-to-end for the full
  archive, plus retries. Increase `--batch-size` (e.g. 200k) to cut load-call count.
- Rate limiting: EA client honours `429 Retry-After` and retries `5xx` with exponential
  backoff+jitter; per-request pacing via `--pace-seconds` (default 0.05s/worker). No
  global token bucket — use 8–16 workers and watch for 429s.

### 10.8 Airflow state
- No Airflow containers are running (`docker ps` empty) and there is no Composer
  instance, so there are **no live DAGs to pause**. The DAG will remain stopped until
  the migration + seed are verified.

## 11. Migration rehearsal in europe-west2 (temporary `*_euw2` datasets)

Datasets created in `europe-west2`: `uk_flood_raw_euw2`, `uk_flood_raw_euw2_staging`,
`uk_flood_euw2`, `uk_flood_euw2_staging`, `uk_flood_euw2_core`, `uk_flood_euw2_marts`.
Global dlt state (`~/.dlt/pipelines/uk_flood_raw`) was reset first (D-010).

**Latest-only reload (2026-10-26, live API):**

| table | rows |
|---|---|
| stations | 5,537 |
| measures | 7,438 |
| readings_latest | 5,376 |
| readings (dual-write) | 5,376 |
| flood_warnings | 114 |
| quarantine | 2 |
| run_metadata | 5 (4 resources + summary) |

**dbt build (BigQuery, temp):** 85 PASS, 1 WARN (known unit whitelist), 0 ERROR;
Elementary monitors executed. `fct_readings`: 5,376 rows, 20 distinct dates,
0 duplicate `(measure_id, reading_time)` keys, partition expiry 90 days
(`expirationMs=7776000000`).

**US vs EU snapshot drift is expected** (live API): measures 7,436 → 7,438,
flood_warnings 1 → 114, readings 5,404 → 5,376; stations equal at 5,537.

**New root causes found during rehearsal:**
1. dlt restored schema state from `~/.dlt` and assumed `run_metadata` existed, so
   the load used `CREATE_NEVER` and 404'd in the new dataset. Reset state fixes it
   (also why BigQuery never had `run_metadata`). Decision D-010.
2. dlt omitted all-NULL columns (`readings.qualifier`, `flood_warnings.situation`),
   breaking staging. Fixed with explicit `TABLE_COLUMNS` + regression test. Decision D-009.

## 12. Full migration + full-archive seed (executed 2026-10-26)

- US datasets dropped; canonical names recreated in `europe-west2`; temp `*_euw2`
  datasets removed after verification.
- Full seed (no client-side filter): **5,376/5,376 measures, 0 failures**,
  **15,429,146 rows**, median 28 days, max 1,820 days, **0 duplicate keys**.
- Raw `readings`: 2021-11-01 → 2026-10-26, 4.34 GB, unpartitioned (retention pending).
- `dbt build --full-refresh`: first run fired an expected post-backfill Elementary
  anomaly; second run **85 PASS / 1 WARN / 0 ERROR / 0 SKIP**.
- `fct_readings`: **14,249,498 rows**, 5,376 measures, 2026-07-28 → 2026-10-26,
  **0 duplicate keys** (90-day partition expiry already trimmed older partitions).
- Idempotency re-run of 100 measures: **0 duplicate keys**; +689 rows were genuinely
  new readings arriving after the seed's max timestamp.
- Marts built: station reliability 5,537; river-level trends 2,581,156;
  rainfall trends 620,229.
- Defaults changed from `US` to `europe-west2` in settings, dbt profile, DAG and
  docker-compose so a missing env var cannot silently target the wrong region.
