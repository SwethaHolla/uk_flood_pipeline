# Phase C baseline: measured BigQuery bytes and tests

Window: `2026-10-25 00:00:00+00` to `2026-10-28 00:00:00+00` (UTC).
Regions: `europe-west2` and `US` (the migration window spans both).
Source: `region-*.INFORMATION_SCHEMA.JOBS_BY_PROJECT`.
Artifacts: `docs/artifacts/bigquery_jobs_*` and `docs/artifacts/dbt_node_costs_*`.

Project identifiers, project numbers, billing account IDs, user emails, and the local key filename were replaced before export. The full sanitized SQL is in the `.jsonl.gz`; the CSV is the compact job index.

## Correction: the 8.3 GB/run figure

**The 8.3 GB/run figure was extrapolated, not measured as a 15-minute DAG run, and it double-counted a seed SCRIPT and its child DELETE.**

The closest measured near-full-size seed batch is:

| Job | Type | Bytes billed | GiB |
|---|---|---:|---:|
| `job_HeHo69EqmBvExO8R8HI7vJhwSvEM` | SCRIPT (DELETE + INSERT) | 4,414,504,960 | 4.11 |
| `script_job_2c93124fe20015e8a0955b9cd8f51b52_0` | child DELETE | 4,358,930,432 | 4.06 |

BigQuery lists the parent SCRIPT and its child statement separately. The parent SCRIPT total already includes the child, so adding 4.41 GB + 4.36 GB is not a run cost. The next seed batches show the same pattern:

| SCRIPT job | SCRIPT bytes | Child DELETE job | DELETE bytes |
|---|---:|---|---:|
| `job_RnVQgplhNZdLvBep95tSQRBhx02G` | 4,380,950,528 | `script_job_da2b88c8e2aa1538d13d6634fdb779b6_0` | 4,349,493,248 |
| `job_4IIjPRNBa660_qjNyhP5aExySrE1` | 4,347,396,096 | `script_job_41afa68fc4e1d9e80a1a59b54b946356_0` | 4,322,230,272 |

The measured standalone dbt `fct_readings` incremental merge is also near-full-size:

| Job | Bytes billed | GiB |
|---|---:|---:|
| `42dcc5b6-7cd2-4e6e-81be-c50aefcfbec8` | 5,246,025,728 | 4.89 |
| `34ce062b-01c2-4186-8f19-fd37bcf5d91b` | 5,246,025,728 | 4.89 |

Therefore:

- Measured dlt merge proxy: **~4.11 GiB per run** (use the SCRIPT total, not SCRIPT + child).
- Measured full ingest + dbt proxy: **~9.0 GiB per run** (4.11 + 4.89).
- At 96 runs/day: **~864 GiB/day** for the combined proxy, **~28.8x** the 30 GiB/day line. The budget conclusion is unchanged; the earlier 8.3 GB figure understated the combined run and overstated the dlt-only run.
- At one-year raw size (~55 GB), a non-pruned dlt merge plus dbt merge is on the order of **~130 GB/run**, or **~12.5 TB/day**; this is why the append-only + partition-pruned design is required before Airflow starts.

## Job totals in the export

The export contains 2,370 job rows. Summing all rows is not the billed total because parent SCRIPT rows overlap child rows.

| Category | Jobs | Sum of rows (bytes) | Note |
|---|---:|---:|---|
| `dlt_delete` | 82 | 172,222,316,544 | child DELETE statements |
| `script` | 119 | 177,901,404,160 | parent SCRIPT statements; includes seed work |
| `dlt_insert` | 86 | 4,630,511,616 | child INSERT statements |
| `dbt` | 715 | 4,631,560,192 | dbt models/tests with destination tables |
| `merge` | 47 | 11,016,339,456 | dbt MERGE statements |
| `select` | 712 | 72,213,331,968 | reads/tests |
| `ctas` | 81 | 1,883,242,496 | table creation |
| `seed_script` | 412 | 41,943,040 | seed control statements |
| `other` | 116 | 188,743,680 | misc |

Use the `category` and `job_id` columns to select a billing role; do not add a parent SCRIPT to its child DELETE/INSERT.

## Actual 2026-10-27 usage

The Phase C analysis day (unit distributions, extreme values, `fct_readings` full refresh, and the scoped dedup/plausibility tests) used:

| Region | Query jobs | Bytes billed | GiB | Slot-ms |
|---|---:|---:|---:|---:|
| `europe-west2` | 118 | 33,195,819,008 | 30.92 | 9,904,934 |
| `US` | 3 | 62,914,560 | 0.06 | 4,012 |
| **Total** | **121** | **33,258,733,568** | **30.98** | **9,908,946** |

This is about **1 GiB above** the documented 30 GiB/day line. No quota error was raised, and the job record is complete. Further BigQuery queries should wait for the daily reset or explicit approval. This measurement also confirms that analysis work alone can consume the daily line; it is not a pipeline run.

## Measured dbt cost per test

These are measured from the same export by parsing the dbt `node_id` in the query comment. Values are per execution, not per day.

| Test | Executions | Bytes/execution (GiB) |
|---|---:|---:|
| `not_null_stg_readings_reading_id` | 4 | 1.30 |
| `unique_stg_readings_reading_id` | 4 | 1.30 |
| `not_null_stg_readings_measure_id` | 4 | 1.05 |
| `assert_no_duplicate_reading_keys` | 4 | 1.05 |
| `relationships_fct_readings_measure_id` | 4 | 0.98 |
| `assert_list_values_always_have_raw` | 4 | 0.10 |
| `assert_reading_time_plausible` | 1 | 0.12 |
| `relationships_dim_measure_station_id` | 5 | 0.03 |

`model.uk_flood.fct_readings` was 13.20 GiB across 4 executions (3.30 GiB/execution average).

The heavy reading tests scan the raw table through the view; they are the first scoping target. The full table is currently 4.34 GB. A three-day filter after partitioning should reduce each of the 1.30 GiB tests to roughly the size of the recent partitions, with the weekly full suite retaining the unbounded check.

## Extreme-value analysis and `is_plausible`

The raw unit labels are not enough on their own: `mm` is used for both rainfall and tidal level. Values are flagged with `is_plausible`; nothing is deleted. Bounds are unit- and measure-type-aware:

| Unit / measure | Bounds |
|---|---|
| `mm` rainfall | -1 to 1,000 |
| `mm` level | -10,000 to 30,000 |
| `mASD`, `m`, `mAOD`, `mBDAT` | -100 to 1,000 |
| `m3/s` | -10,000 to 10,000 |
| `Ml/d` | -1,000,000 to 1,000,000 |
| `deg C` | -30 to 60 |
| `deg` | -360 to 360 |
| `m/s` | 0 to 100 |
| `%` | 0 to 100 |
| `Knots` | -1 to 200 |
| `V` | 0 to 100 |
| null value / unknown unit | true (not flagged) |

Measured flags in the full archive (15,429,835 rows):

| Unit | Flagged |
|---|---:|
| `deg` | 196 |
| `mm` | 20 |
| `mASD` | 17 |
| `deg C` | 7 |
| `m` | 3 |
| `mAOD` | 2 |
| `m3/s` | 1 |
| `Ml/d`, `mBDAT`, `m/s`, `%`, `Knots`, `V`, `---` | 0 |

Examples: `19,999,290` and `19,998,940` mm rainfall; `999,891`/`999,842`/`999,700` mm rainfall; `65,535`/`63,497`/`40,280` m stage; `-1,055.068` mASD; `7,729` mAOD; `381.664` deg; `-99.99` deg C; `10,767.827` m3/s. The `E72825` tidal-level series at 12,000-13,000 mm is **not** flagged; it is a level in mm, not rainfall.

The `fct_readings` full refresh added the column in `CREATE TABLE (14.2m rows, 3.4 GiB processed) in 25.42s`; the 14.2m row count reflects the 90-day partition expiry, not dropped raw history. The new `dbt/tests/assert_recent_readings_plausible.sql` warn test reports **17** recent flagged rows (last three days): 12 `deg`, 3 `deg C`, 1 `mm`, 1 `m3/s`. The 164 `Ml/d` readings near 11,000 were removed from the flag set after checking that 11,000 Ml/d is about 127 m3/s (a plausible large-river flow).

## Test scoping

Daily `dbt test` runs now use `dbt/macros/test_lookback.sql`: default lookback is 3 days (`test_lookback_days`), and the SQL is portable to both BigQuery and DuckDB (`current_timestamp - interval N day`). The scoped tests are the four expensive `stg_readings` checks, the `fct_readings` key checks, and the singular reading checks. `make dbt-test` and `make dbt-build` therefore test recent partitions by default.

The weekly full-history run is an explicit target:

```bash
make dbt-test-full
```

It passes `--vars '{test_lookback_days: 100000}'`, which moves the predicates back to 1751 and therefore scans every archived row. `dbt-test-full` is intentionally not part of the 15-minute path. The heavy tests previously cost up to 1.30 GiB per run each.

Important: `stg_readings` is a view over the unpartitioned raw table, so its `reading_time` filter does **not** prune bytes today; it only reduces returned rows. The `fct_readings` filters on `reading_date` do prune because that table is partitioned. The stg filters become effective only after the Phase C raw partition migration.

## Config and adapter findings

- **dbt max bytes:** `dbt/profiles.yml:11` sets `maximum_bytes_billed` from `MAX_BYTES_BILLED`, default `10737418240` (10 GiB). `.env.example:13` and `docker/docker-compose.yml:20` carry the same default. It is a profile-level guard, not a per-model limit.
- **dlt max bytes:** no `max_bytes_billed` support was found in the installed dlt 1.31.0 source. dlt BigQuery loads use load jobs; the merge follow-up is a query job without a max-bytes option. Removing the dlt merge in favour of append removes this gap.
- **dlt partitioning:** dlt 1.31 supports `bigquery_adapter(resource, partition="datetime", cluster=["measure"])` and `x-bigquery-partition` / `x-bigquery-cluster` hints. The public BigQuery destination docs document `partition` and `cluster`; they do not document merge partition pruning.
- **dlt merge pruning:** installed source contains `x-merge-destination-scope` and `x-merge-source-filter` handling in `SqlMergeFollowupJob.gen_merge_partition_clauses`; these are undocumented in the destination page and are not the primary design.
- **dbt incremental predicates:** installed dbt-bigquery 1.9.2 supports `incremental_predicates`; `dbt/include/bigquery/macros/materializations/incremental_strategy/merge.sql:24` passes them into `get_merge_sql`. This is the supported Phase F fallback for a partition-filtered MERGE.
- **Elementary:** installed Python CLI is 0.26.1; the dbt package is pinned at 0.16.4 (`packages.yml: >=0.13.0,<0.17.0`). The current dbt package works for `dbt build`, but the CLI/package versions are not aligned; Elementary's current quickstart uses package 0.26.x with CLI 0.26.x. Upgrade the package and CLI together in Phase C, or pin the CLI to the package generation. The mismatch does not affect the existing dbt tests; it can affect CLI report compatibility.

## Security and provenance

- Full-history gitleaks scan: 26 commits, no leaks found. The pre-commit gitleaks hook also passes on the working tree. No `--no-verify` was used.
- Bounds tests were committed on `phase-c-hardening` in `6bdee93` after the hooks passed.
- The local service-account key remains untracked and gitignored; it is not in the artifacts.
