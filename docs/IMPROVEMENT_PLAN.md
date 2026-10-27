# Improvement Plan — UK Flood Pipeline

**Status:** Phase A audit. No code changed yet. Awaiting sign-off.
**Author:** audit pass (senior DE review).
**Date:** 2026-10-22
**Repo commit at audit:** `18e7a0a` (branch `main`), audit branch `phase-a-audit`.
**Environment:** GCP project `ukfloodpipeline`, BigQuery location **US** (multi-region), Python 3.12, dbt-core 1.9.x, dlt 1.x, Elementary 0.16.4.

> Policy reminder for the whole programme: every number in docs must be produced by a
> script and carry **date + commit SHA + environment**. No hand-edited numbers. Where
> a benchmark cannot clear BigQuery's minimum-billing floor it is **not** evidence.

---

## 1. What exists (verified this audit)

### 1.1 Code / repo
- `src/uk_flood/` — `contracts.py` (Pydantic), `ea_client.py` (httpx + tenacity retries), `pipelines.py` (dlt, quarantine, run_metadata, dual-write `readings_latest`→`readings`), `settings.py`, `ingest.py` (Typer CLI).
- `dbt/` — staging (6 models), core (5: `dim_station`, `dim_measure`, `dim_flood_area`, `fct_readings`, `fct_flood_warnings`), marts (4), 4 singular tests, `clean_categorical` macro. Incremental `fct_readings` uses adapter-gated strategy (BigQuery MERGE / DuckDB delete+insert), partition `reading_date` DAY, cluster `measure_id`.
- `dags/uk_flood_pipeline.py` — single DAG, `*/15 * * * *`, tasks `ingest_raw_data → dbt_build → elementary_monitoring`.
- `docker/` — Dockerfile.airflow (Airflow 2.10.4/py3.12) + compose (Postgres, LocalExecutor).
- `mock_api/` — FastAPI mock with quirk fixtures. `scripts/` — `ingest_local.py`, `seed_history.py`, `chaos.py`, `benchmark.py`.
- `tests/` — 38 offline tests (client, contracts, idempotency, ingest, quarantine, settings).
- `.github/workflows/ci.yml` — lint, test, dbt parse, sqlfluff, fresh-clone.

### 1.2 Live BigQuery state (queried this audit via SA `dlt-runner`)
| Table | Rows | Bytes |
|---|---|---|
| `uk_flood_raw.readings_latest` | 5,404 | 1,556,284 |
| `uk_flood_raw.readings` | 5,404 | — |
| `uk_flood_raw.stations` | 5,537 | — |
| `uk_flood_raw.measures` | 7,436 | — |
| `uk_flood_raw.flood_warnings` | 1 | — |
| `uk_flood_raw.quarantine` | 2 | — |
| `uk_flood_core.fct_readings` | 5,404 | 1,676,803 |

- `fct_readings` reading_time range: **2026-09-22 06:30Z → 2026-10-21 18:55Z** (~29 days).
- Datasets present: `uk_flood`, `uk_flood_raw`, `uk_flood_core`, `uk_flood_marts`, `uk_flood_staging`, `uk_flood_raw_staging` (+ Elementary tables inside `uk_flood`).

### 1.3 What's genuinely good (keep, don't rebuild)
- Contract + quarantine design and idempotent dlt merge keys are sound.
- dbt layer is well-structured; the one WARN is correctly scoped (below).
- Offline path (mock API + DuckDB) is real and CI-gated.
- Chaos + benchmark scripts exist as a starting point.

---

## 2. The 1 dbt WARN — explained

```
Warning in test accepted_values_stg_measures_unit_name__m__Cumecs__mm___C__m_s__l_s__nt__knots
Got 15 results, configured to warn if != 0
```

`stg_measures.unit_name` is tested against whitelist
`['m','Cumecs','mm','°C','m3/s','l/s','nt','knots']` with `severity: warn` (intentional
per `models/staging/schema.yml`). **15 measure rows carry a unit outside the whitelist.**
Root cause: the EA API invents units beyond the documented set (PRD §7 quirk 8/9). This
is a legitimate warn, not a bug — but it is currently **undocumented in the README** and
the raw offending values are not surfaced anywhere. Action: keep the warn, but (a) route
the offending values into the observability layer so the count is visible over time, and
(b) document it as an expected-quality signal.

---

## 3. Gaps vs the brief (what is missing)

### 3.1 Consistency / safety
1. **Location mismatch (GATE).** Everything is in **US**; the decision recorded was
   **europe-west2**. Dataset location is immutable — this needs a recreate + reload
   migration (Phase B).
2. **Key-file guidance.** `.env.example`, `dbt/profiles.yml` and the DAG all lean on
   `GOOGLE_APPLICATION_CREDENTIALS` (a key file). A `ukfloodpipeline-key.json` exists in
   the working tree (untracked, gitignored — verified **not** in git history). Brief
   wants SA **impersonation** or ADC, least-privilege, no key files, and documented
   Airflow-in-Docker auth.
3. **`maximum_bytes_billed`** is in `profiles.yml` (dbt) and `settings.py` (10 GiB
   default) but **not enforced on dlt/BigQuery load jobs** and not measurable per query.
   No documented project **daily quota** (30 GiB/day) or £5 budget reality-check.
4. **Retention:** no partition expiry anywhere. Raw/staging readings grow unbounded.
5. **Secret scan:** need a real scanner pass (gitleaks/`git grep`) for billing IDs,
   project number, keys. Project number `371359095978` and billing account id appear in
   earlier *session notes* but I must confirm none are committed.

### 3.2 Observability (currently thin)
- `run_metadata` + `quarantine` exist in **raw** but there is **no `ops` dataset**, no
  per-run SLIs/SLOs, no structured JSON logging with `run_id`/`load_id` correlation, no
  BigQuery job labels, no ingestion of dbt `run_results.json` into ops, no ops views.
- Elementary: only **3 monitors** (`column_anomalies` null_rate/max/min + two
  `table_anomalies`). No schema-change monitor on core; anomaly tests run **without a
  configured training window** and on ~1 month of thin data → cannot honestly claim
  "passed" baseline.
- No silent-failure detection (green run, zero rows). No alerting callbacks. No Airflow
  SLA/timeout handling.

### 3.3 Orchestration / resilience
- One 15-min DAG runs **dbt build every 15 min** (cadence not justified, wasteful).
- No gap-healing backfill; `seed_history.py` is manual and not DAG-wired.
- Idempotency is tested offline but **not proven on BigQuery as a row-count+checksum
  test**.
- Version pins are ranges (`dbt-bigquery>=1.8,<1.10`, `elementary-data>=0.13`) — need to
  check current releases and decide deliberately. Checked this audit: current
  **Airflow 3.3.2** (repo pins 2.10.4 — upgrade is a major version, high-risk, plan
  first), **Elementary 0.26.1** (repo 0.16.4). Decision deferred to Phase D with a
  written upgrade/no-upgrade rationale.

### 3.4 Chaos / benchmarks
- Chaos: 7 transport-level scenarios only; **no schema drift, type change, duplicates,
  late/out-of-order, empty, partial payload, quarantine-flood**. Also the current run
  produced a results file where **every scenario recorded `error`** (mock server wasn't
  reachable in that run) yet still wrote `[OK] Handled` — a reporting bug: the table
  claims success on a connection refusal. This must be fixed.
- Benchmarks: single run per query, **no N, no median/p95, no `INFORMATION_SCHEMA.JOBS`
  slot_ms, no dry-run pruning check, no synthetic scale tier, no variant comparison, no
  before/after**, and the `docs/benchmarks.md` we generated listed every query pinned at
  the **10 MB minimum-billed floor** (1,676,803 bytes processed but 10,485,760 billed).
  At that floor the numbers are **not evidence** and the `$5/TB` rate was wrong
  (official on-demand is **$6.25/TiB**, first 1 TiB/month free, 10 MB min/table+query).

### 3.5 dbt rigor / CI parity
- No **model contracts** (enforced) on core; no **dbt unit tests**; no **exposures**;
  only 4 singular tests (brief wants 2-3 more: impossible levels, stale stations, gaps).
- Adapter parity: DuckDB delete+insert vs BigQuery merge is real but **untested on
  BigQuery in CI**; no WIF-backed BQ CI job.

### 3.6 Docs / dashboard
- `docs/` was deleted in commit `18e7a0a`; need `IMPROVEMENT_PLAN.md`, `SLOS.md`,
  `DECISIONS.md`, `NEXT_STEPS.md`, `BENCHMARK_PROTOCOL.md`, `chaos-results.md`,
  `benchmarks.md` regenerated as artifacts.
- No Looker Studio views/setup, no GitHub Pages for Elementary/dbt docs, no external
  freshness monitor.
- README claims "real-time", "production-grade", cites `$5/TB`, and benchmarks at the
  billing floor — all must be corrected to artifact-backed statements.

---

## 4. Contradictions with earlier decisions
- **Location**: recorded decision europe-west2 vs deployed US.
- **Benchmark rate**: README `$5/TB` vs official `$6.25/TiB` (and free tier).
- **"Elementary passed"**: claimed passed on a 1-month, no-training-window baseline.
- **`.gitignore` `*.json`**: currently the ONLY thing protecting the SA key. That is
  fragile (a `!` negation or a move breaks it) and there is no scanner in CI.

## 5. Risks
| Risk | Severity | Note |
|---|---|---|
| Location recreate loses all data | High | Migrate + reload; only ~5.4k readings + 29 days, cheap to redo. |
| Quota/budget vs benchmark scan | Medium | Phase F gate will dry-run total bytes before running. |
| Billing account id / project number leak | High | Add scanner + confirm history clean. |
| Synthetic 100x data mistaken for real | High | Labelled + separate dataset everywhere. |
| Anomaly "pass" on empty baseline misleads | Medium | Configure training window / document thin-baseline behaviour. |

## 6. Proposed phase order (unchanged from brief)
A (this) → B consistency/safety → C observability → D orchestration → E chaos v2 →
F benchmarks → G dashboard/pages → H dbt rigor/CI → I README/final.

## 7. Decision — early corrections before any new features
I recommend folding two corrections into **Phase B** (not wait): (1) fix the
chaos-reporting bug, (2) stop citing `$5/TB`. Both are cheap and stop the repo
asserting things that aren't true.
