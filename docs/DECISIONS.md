# DECISIONS.md

Running log of decisions and *why*. Newest first.

## 2026-10-12 — Phase 0 discovery (verified live)
- **API**: UK EA Flood Monitoring, no key, OGL v3, beta/no-SLA. Endpoints and
  live counts: `/id/floods` 7, `/id/stations` 5538, `/id/measures` 7429,
  `/data/readings?latest` 5412.
- **Quirks confirmed by inspection** (see PRD §7): reading `value` can be a list
  (`[0.0,0.0]`) and can be omitted (NaN); station `lat`/`easting` include list
  values and nulls; floods timestamps lack timezone while readings use `Z`;
  many sparse station/measure fields; dirty categoricals (`'---'`, `''`,
  `'None'`); odd parameters (`people_count`, `freeze_area`...); `?since` is
  per-measure/station only (global → 400); `?station=` needs a URI, reference
  gives 500; readings hard cap 10000 rows; history only ~4 weeks (archive CSVs
  elsewhere). No `@id` duplicates in any snapshot.
- **Environment (corrected from prior notes)**: Docker **is** present (29.8.2,
  running); gcloud **is** present (588.0.0, authed); uv 0.11.15 present;
  Python 3.13 + 3.12 installed (no 3.11); **gh installed but not authenticated**;
  GCP project `ukfloodpipeline` exists but **billing disabled, 0 APIs enabled**.
- **Decision (proposed, awaiting sign-off)**: pin project to **Python 3.12** via
  uv for Airflow compatibility; run Airflow in Docker as specified.
- **BigQuery (proposed, awaiting sign-off)**: recommend **(a) billing-enabled
  project + budget alert** over (b) sandbox, because sandbox forbids **DML**,
  which breaks dlt `merge`, dbt incremental (MERGE) and Elementary, and imposes
  60-day expiry. Tradeoff documented in PRD §12 Q1. No cloud action taken yet.
- **No tools installed; nothing committed; no repo created.**
