# NEXT_STEPS.md

Open threads. Update at the end of each phase.

## Blocking the Phase 0 sign-off
- [ ] Decide BigQuery option **(a) billing-enabled + budget alert** vs **(b)
      sandbox + re-seed** (PRD §12 Q1). Billing currently disabled on
      `ukfloodpipeline`; sandbox forbids DML.
- [ ] Decide retention: do we need data >60 days? (PRD Q2)
- [ ] Confirm budget-alert threshold + payment-method OK (PRD Q3).
- [ ] Confirm repo name / visibility / owner (PRD Q4).
- [ ] Alerting channel: Slack or email (PRD Q5).
- [ ] History scope: `?latest` only, or also the ~4-week window (PRD Q6).
- [ ] `gh auth login` before Phase 1 (or fallback manual remote) (PRD Q7).

## Blocking Phase 1 start
- [ ] `gh auth login` (currently unauthenticated).
- [ ] If option (a): enable billing + BigQuery API on `ukfloodpipeline` — will
      show a plan and wait for "proceed" (billable/irreversible-ish).

## Later / watch
- [ ] Confirm dlt BigQuery write dispositions actually used (append vs merge vs
      replace) once ingestion is built; document each.
- [ ] If option (b) chosen: design load-job/replace workarounds for dlt merge,
      dbt incremental and Elementary, plus re-seed script.
- [ ] Verify whether `type`/`status` fields appear only under filters or
      `_view=full` on stations (need this for the contract).
