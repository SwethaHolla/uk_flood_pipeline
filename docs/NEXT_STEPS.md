# NEXT_STEPS.md

**Status:** Phases 1-4 complete, moving to 5-7

## Completed
- [x] Phase 0: Discovery & PRD
- [x] Phase 1: Repo scaffolding, CI, Docker/Airflow setup
- [x] Phase 2: dlt ingestion + Pydantic contracts + quarantine
- [x] Phase 3: dbt staging/core/marts + tests + Elementary (verified on BigQuery)
- [x] Phase 4: Airflow DAGs (ingest -> dbt -> Elementary), chaos & benchmark scripts
- [x] BigQuery enabled with billing, APIs enabled, live data ingested
- [x] README comprehensive, all PRs merged

## Phase 5: Observability & Alerting
- [ ] Generate Elementary report: `make elementary-report`
- [ ] Configure alerting channel (Slack webhook or email) - decide
- [ ] Validate `run_metadata` and `quarantine` tables populated correctly
- [ ] Set up budget alert in GCP Console (£1 threshold recommended)

## Phase 6: Chaos & Performance
- [ ] Run chaos harness against mock API: `make chaos`
- [ ] Document chaos results in `docs/chaos-results.md`
- [ ] Run benchmarks against BigQuery: `python scripts/benchmark.py`
- [ ] Document benchmarks in `docs/benchmarks.md`

## Phase 7: Polish & Documentation
- [ ] Final verification: `make ci` passes locally
- [ ] Document dlt write dispositions per resource
- [ ] Update `docs/DECISIONS.md` with all key decisions
- [ ] Verify all PRD acceptance criteria met
- [ ] Tag release if desired

## Open Questions (resolved)
- [x] BigQuery: option (a) billing-enabled - done
- [x] Repo: `https://github.com/SwethaHolla/uk_flood_pipeline.git` public
- [x] `gh` authenticated
- [x] History scope: using `?latest` + `seed_history.py` for backfill

## Notes
- Airflow can be started with `make up` from `docker/`
- All phases 1-3 verified against real BigQuery project `ukfloodpipeline`
- Elementary monitors gated to BigQuery target only

