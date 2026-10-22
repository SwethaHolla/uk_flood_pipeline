# Decisions Log — UK Flood Pipeline

Recorded decisions, rationale, and status. Newest entries at the bottom of each
section. Every entry states the evidence it rests on.

---

## D-001: BigQuery location — US (current) vs europe-west2 (target)

- **Status:** OPEN — GATE.
- **Context:** all datasets (`uk_flood`, `uk_flood_raw`, `uk_flood_core`,
  `uk_flood_marts`, `uk_flood_staging`, `uk_flood_raw_staging`) are in the **US**
  multi-region. The earlier recorded intent was **europe-west2**.
- **Constraint:** BigQuery dataset location is immutable after creation.
- **Decision:** migrate by recreating datasets in europe-west2 and reloading.
  Data is tiny (~7.5k rows/dataset, ~3.3 MB total), so recreate+reload is cheap.
- **Not done yet:** no dataset created/dropped/moved. Waits for explicit approval.

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

- **Status:** decided (sequence).
- **Evidence:** the **python** package in `.venv` is already `elementary-data 0.26.1`,
  but the **dbt package** is pinned `elementary-data/elementary [">=0.13.0","<0.17.0"]`
  (locked `0.16.4`). These are different artefacts.
- **Rationale:** upgrade only after confirming dbt-core compatibility
  (`0.16.4` works with dbt-core 1.9.x), and **before** any baselines are
  established, so a package change cannot invalidate accumulated anomaly metric
  history. Bump `packages.yml` + `package-lock.yml` in an isolated commit and
  re-run `dbt deps`/`dbt parse`.

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
