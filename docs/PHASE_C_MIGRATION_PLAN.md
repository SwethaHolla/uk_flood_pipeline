# Phase C: append-only raw + idempotent downstream

Status: **draft for approval**; nothing in this plan has been executed.
Airflow remains off until the measured post-change cost is under ~15 GiB/day.

## Decisions already taken

- Keep the full raw archive.
- Raw is append-only and partitioned by `DATE(datetime)`, clustered by `measure`.
- dlt lands append-only; it does not delete or merge raw.
- dbt deduplicates in the transformation layer by `(measure_id, reading_time)`, keeping the row from the latest `_dlt_load_id`.
- Duplicate rows are allowed in raw and tracked as an ops metric. The README claim is "at-least-once landing, idempotent downstream", not "zero duplicates in raw".
- Show this plan before executing the table swap.

## Why the current path cannot be scheduled

Measured on 2026-10-25..27, not extrapolated:

| Item | Bytes |
|---|---:|
| dlt seed `SCRIPT` `job_HeHo69EqmBvExO8R8HI7vJhwSvEM` | 4,414,504,960 (4.11 GiB) |
| its child `DELETE` `script_job_2c93124fe20015e8a0955b9cd8f51b52_0` | 4,358,930,432 (same work; do not add) |
| dlt seed `SCRIPT` `job_RnVQgplhNZdLvBep95tSQRBhx02G` | 4,380,950,528 |
| its child `DELETE` `script_job_da2b88c8e2aa1538d13d6634fdb779b6_0` | 4,349,493,248 (same work; do not add) |
| dbt `fct_readings` merge `42dcc5b6-7cd2-4e6e-81be-c50aefcfbec8` | 5,246,025,728 (4.89 GiB) |
| dbt `fct_readings` merge `34ce062b-01c2-4186-8f19-fd37bcf5d91b` | 5,246,025,728 |

The old 8.3 GB/run number was extrapolated and double-counted the dlt SCRIPT and its child DELETE. The measured proxy is **~9.0 GiB per run** (4.11 dlt merge + 4.89 dbt merge), or **~864 GiB/day at 96 runs/day** (~28.8x the 30 GiB/day line). At one-year raw size (~55 GB), the same unpinned merges scale to roughly 130 GB/run and 12.5 TB/day. This is why Airflow is off. The Phase C analysis day itself used **30.98 GiB** (121 query jobs, 2026-10-27), about 1 GiB over the documented daily line; no further BigQuery queries should run until the daily reset or explicit approval.

## Target shape

### Raw `readings`

```sql
CREATE TABLE `ukfloodpipeline.uk_flood_raw.readings_partitioned`
PARTITION BY DATE(datetime)
CLUSTER BY measure
AS SELECT * FROM `ukfloodpipeline.uk_flood_raw.readings`;
```

- Same columns, including `_dlt_load_id` and `_dlt_id`.
- Partition column: `datetime` (TIMESTAMP).
- Cluster column: `measure` (the EA measure URI).
- No primary key; duplicates are expected and allowed.
- dlt hint: `bigquery_adapter(readings, partition="datetime", cluster=["measure"])`.
- dlt write disposition: `append`; no `merge`, no raw `DELETE`.

### `fct_readings`

- Keep `reading_date` DAY partition, `measure_id` cluster, 90-day expiry.
- Replace the merge with partition-scoped `insert_overwrite` over the last N days.
- Deduplicate before insert:

```sql
qualify row_number() over (
    partition by measure_id, reading_time
    order by _dlt_load_id desc, _dlt_id desc
) = 1
```

- Keep `unique_key` as documentation, but idempotency comes from partition replacement + dedup, not from a raw merge.
- Keep `is_plausible`; the Phase C full refresh populates historical rows.
- Add a `raw_readings_duplicates` metric (count by `(measure, datetime)`) so at-least-once behavior is visible rather than hidden.

## Migration steps (no destructive step without approval)

1. **Freeze and snapshot**
   - Confirm Airflow is down.
   - Record raw row count, min/max `datetime`, per-partition counts, duplicate counts, and `INFORMATION_SCHEMA` size.
   - Keep the local SA key out of the repository and out of the artifacts.

2. **Build the partitioned copy**
   - Run the CTAS above into `readings_partitioned`.
   - Verify row counts, min/max, per-partition counts, checksums over `(measure, datetime, value_raw)`, and duplicate counts against the old table.
   - Do not drop or rename yet.

3. **Rehearse dlt append**
   - Point dlt at `readings_partitioned` in a temporary adapter/config.
   - Run one controlled ingestion; assert the row count increases, no rows are deleted, and the partition/cluster layout is preserved.
   - Inspect the dlt job: it must be a load job, not a merge query.
   - Discard the rehearsal rows or run it against a copy; do not pollute the production raw table during the rehearsal.

4. **Rehearse dbt**
   - Point the dbt source at `readings_partitioned` on a branch/target.
   - Compile and run `fct_readings` with the new `insert_overwrite` config.
   - Compare `fct_readings` row counts, duplicate counts, min/max, and `is_plausible` flags against the current table.
   - Measure bytes for the first run and for a repeat run (repeat must be cheaper and idempotent).
   - Test both `copy_partitions` settings if the first setting is not clearly cheaper.

5. **Swap names**
   - Keep the old table as `readings_pre_append_20261027`.
   - Rename `readings_partitioned` to `readings`.
   - Update dbt source and dlt config to the new name if needed.
   - Verify one dlt append and one dbt run.
   - Keep the old table for at least 7 days; drop only after the new path is measured and approved.

6. **Validate**
   - Recent `dbt test` (3-day lookback), then one weekly full suite (`make dbt-test-full`).
   - `assert_no_duplicate_reading_keys` on `fct_readings` must pass.
   - Raw duplicate metric is reported, not treated as a failure.
   - Elementary: run dbt twice after the full refresh before judging min/max anomalies (post-backfill rule).
   - Record per-test and per-model bytes; update the baseline doc.

7. **Only then schedule Airflow**
   - Start with dbt hourly, not every 15 minutes, unless the measured bytes prove a 15-minute dbt run fits the budget.
   - Keep ingestion at 15 minutes if needed; dbt cadence is a separate decision.
   - Re-measure after 24 hours and compare with the 15 GiB/day line.

## Cost model (estimates, to be measured)

Raw growth is ~490,623 rows/day, ~0.14 GB/day at 281 B/row; 3 days of raw is ~0.42 GB. A 3-partition `insert_overwrite` therefore scans/writes roughly 0.4-0.5 GB per dbt run before column pruning.

| dbt cadence | Runs/day | 3-partition overwrite (rough) | Append landing | Under 15 GiB/day? |
|---|---:|---:|---:|---|
| 15 min | 96 | ~40-48 GB/day | load jobs | No |
| 30 min | 48 | ~20-24 GB/day | load jobs | No |
| hourly | 24 | ~10-12 GB/day | load jobs | Yes, if tests stay small |
| hourly, 2 partitions | 24 | ~7-8 GB/day | load jobs | Yes, more margin |

These are models, not measurements. The plan is to measure the hourly / 3-partition path first; if tests push it over 15 GiB/day, reduce to 2 partitions or move the fct cadence to 2 hours. The 30 GiB/day documented quota is not a target; the target is 15 GiB/day.

Test scoping is already implemented: daily tests use a 3-day lookback; `make dbt-test-full` runs the full archive weekly. The `stg_readings` filters do not prune today because raw is unpartitioned; they should prune after this migration.

## Risks and checks

- **dlt does not honor the partition hint on an existing table.** Rehearse against a copy; inspect the created table's partitioning and clustering. If dlt rewrites the table, create it explicitly first.
- **`insert_overwrite` deletes more than intended.** Use an explicit `partitions` list; do not rely on an unbounded predicate. Test on a copy.
- **Late/out-of-order readings.** Keep N >= 3 days (or prove the API's maximum lateness). The dedup key is unaffected; only the partition window matters.
- **Duplicate semantics.** The fct dedup keeps the latest load; if the same `_dlt_load_id` is retried with different `_dlt_id`, the tie-break is deterministic (`_dlt_id desc`).
- **Test cost after partitioning.** Re-measure the five heavy tests; the stg filters should prune only after the view is over the partitioned table.
- **Elementary false positives.** Full refresh resets min/max history; run twice before interpreting anomalies.
- **Rollback.** Old raw table retained 7 days; dbt config revert is a one-line change; dlt append can write to the old table if needed.

## Open decisions before execution

1. N for `insert_overwrite`: 2 or 3 days (3 is safer for late data; 2 is cheaper).
2. dbt cadence: hourly vs 2-hourly.
3. `copy_partitions`: test both, choose the cheaper correct setting.
4. Whether the raw duplicate metric is a dbt model, a singular test output, or both.
5. Exact 7-day retention/drop date for `readings_pre_append_20261027`.
