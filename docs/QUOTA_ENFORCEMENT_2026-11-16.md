# BigQuery quota enforcement evidence: 2026-11-16

Window: **2026-11-16 08:00 UTC** (start of the QueryUsagePerDay quota day) to **2026-11-17 01:00 UTC**.

Source: `region-europe-west2.INFORMATION_SCHEMA.JOBS_BY_USER` for the runtime service account and the operator account. A project-wide `JOBS_BY_PROJECT`/`JOBS` query is denied because the runtime SA lacks `bigquery.jobs.listAll`; the totals below therefore exclude any other principals.

- Jobs: **2,712**
- Total bytes billed: **40,407,924,736 B** (37.63 GiB)
- Total bytes processed: **38,973,258,464 B** (36.30 GiB)
- Quota: **31,457 MiB/day** (30.72 GiB)
- Billed over quota: **7,422,869,504 B** (6.91 GiB)

## Quota error (exact text, redacted)

```text
Custom quota exceeded: Your usage exceeded the custom quota for QueryUsagePerDay, which is set by your administrator. For more information, see https://docs.cloud.google.com/bigquery/cost-controls. To update the limit, go to https://docs.cloud.google.com/bigquery/redirects/increase-query-cost-quota. at [1:1]
```

This message appears on 2,278 failed runtime-SA jobs (1,139 with and 1,139 without the trailing `at [1:1]` location suffix).

## By principal

| Principal | Jobs | Bytes billed | GiB | Bytes processed |
|---|---:|---:|---:|---:|
| runtime SA | 2,695 | 32,456,572,928 | 30.23 GiB | 31,064,472,721 |
| operator account | 17 | 7,951,351,808 | 7.41 GiB | 7,908,785,743 |

## By statement type

| Statement type | Jobs | Bytes billed | GiB |
|---|---:|---:|---:|
| SCRIPT | 1,175 | 11,687,428,096 | 10.88 GiB |
| SELECT | 277 | 10,133,438,464 | 9.44 GiB |
| DELETE | 1,149 | 7,466,909,696 | 6.95 GiB |
| CREATE_TABLE_AS_SELECT | 11 | 7,012,876,288 | 6.53 GiB |
| MERGE | 2 | 3,855,613,952 | 3.59 GiB |
| INSERT | 37 | 251,658,240 | 0.23 GiB |
| (none) | 22 | 0 | 0.00 GiB |
| CREATE_TABLE | 4 | 0 | 0.00 GiB |
| TRUNCATE_TABLE | 9 | 0 | 0.00 GiB |
| CREATE_VIEW | 11 | 0 | 0.00 GiB |
| DROP_TABLE | 2 | 0 | 0.00 GiB |
| BEGIN_TRANSACTION | 4 | 0 | 0.00 GiB |
| COMMIT_TRANSACTION | 4 | 0 | 0.00 GiB |
| CREATE_SCHEMA | 1 | 0 | 0.00 GiB |
| CREATE_EXTERNAL_TABLE | 2 | 0 | 0.00 GiB |
| DROP_EXTERNAL_TABLE | 2 | 0 | 0.00 GiB |

## By dbt invocation label

| Label | Jobs | Bytes billed | GiB |
|---|---:|---:|---:|
| (none) | 2,641 | 24,773,656,576 | 23.07 GiB |
| dbt_invocation_id=acc93142-c771-4cb0-88b0-b597eedbcb0c | 37 | 13,686,013,952 | 12.75 GiB |
| dbt_invocation_id=d8c0b709-071f-4885-b05b-07f1f6937ab4 | 22 | 1,631,584,256 | 1.52 GiB |
| dbt_invocation_id=da34fa4c-66b4-4185-93cf-349b5938b9eb | 6 | 316,669,952 | 0.29 GiB |
| dbt_invocation_id=a318713a-0ad0-4b58-ac6e-2c784520f263 | 1 | 0 | 0.00 GiB |
| dbt_invocation_id=cd036edc-860a-4052-8a6e-8b99f0c98640 | 5 | 0 | 0.00 GiB |

## Top query-text groups

| Principal | Type | Billed | Query text (first 180 chars, redacted) |
|---|---|---:|---|
| runtime SA | SCRIPT | 7,341,080,576 | `DELETE FROM `<GCP_PROJECT_ID>`.`uk_flood_raw`.`readings` AS d WHERE EXISTS (SELECT 1 FROM `<GCP_PROJECT_ID>`.`uk_flood_raw_staging`.`readings` AS s WHERE d.`measure` = s.`measure` AN` |
| runtime SA | DELETE | 7,299,137,536 | `DELETE FROM `<GCP_PROJECT_ID>`.`uk_flood_raw`.`readings` AS d WHERE EXISTS (SELECT 1 FROM `<GCP_PROJECT_ID>`.`uk_flood_raw_staging`.`readings` AS s WHERE d.`measure` = s.`measure` AN` |
| operator account | SELECT | 7,229,931,520 | `SELECT 'readings' AS table_name, COUNT(*) AS row_count, COUNT(DISTINCT _dlt_id) AS distinct_dlt_ids, MIN(datetime) AS min_dt, MAX(datetime) AS max_dt, COUNTIF(measure IS NULL OR da` |
| runtime SA | CREATE_TABLE_AS_SELECT | 6,981,419,008 | `create or replace table `<GCP_PROJECT_ID>`.`uk_flood_core`.`fct_readings__dbt_tmp` partition by reading_date cluster by measure_id OPTIONS( expiration_timestamp=TIMESTAMP_ADD(CURREN` |
| runtime SA | SCRIPT | 3,937,402,880 | `-- generated script to merge partitions into `<GCP_PROJECT_ID>`.`uk_flood_core`.`fct_readings` declare dbt_partitions_for_replacement array<date>; -- 1. temp table already exists, w` |
| runtime SA | MERGE | 3,855,613,952 | `merge into `<GCP_PROJECT_ID>`.`uk_flood_core`.`fct_readings` as DBT_INTERNAL_DEST using ( select * from `<GCP_PROJECT_ID>`.`uk_flood_core`.`fct_readings__dbt_tmp` ) as DBT_INTERNAL_S` |
| runtime SA | SELECT | 1,209,008,128 | `SELECT COUNT(*) n, MIN(datetime) min_dt, MAX(datetime) max_dt, COUNT(DISTINCT _dlt_id) distinct_ids, MAX(_dlt_load_id) max_load FROM `<GCP_PROJECT_ID>.uk_flood_raw.readings`` |
| operator account | SELECT | 330,301,440 | `SELECT 'fct_readings' AS table_name, COUNT(*) AS row_count, MIN(reading_time) AS min_dt, MAX(reading_time) AS max_dt FROM `<GCP_PROJECT_ID>.uk_flood_core.fct_readings` UNION ALL SEL` |
| runtime SA | SELECT | 251,658,240 | `select artifacts_model, metadata_hash from `<GCP_PROJECT_ID>`.`uk_flood`.`dbt_artifacts_hashes` order by metadata_hash` |
| operator account | SELECT | 247,463,936 | `SELECT 'readings_legacy_backup' AS source, COUNT(*) AS row_count, MIN(datetime) AS min_dt, MAX(datetime) AS max_dt FROM `<GCP_PROJECT_ID>.uk_flood_raw.readings_legacy_backup_check` ` |
| runtime SA | SELECT | 163,577,856 | `select count(*) as failures, count(*) != 0 as should_warn, count(*) != 0 as should_error from ( -- Staging owns raw dedup: keep the latest _dlt_load_id per (measure, datetime). -- ` |
| runtime SA | SCRIPT | 136,314,880 | `CREATE TEMPORARY TABLE stations_delete_bb88daa3d782ea8f3743189b0ca2e722 AS SELECT d.`_dlt_id` FROM `<GCP_PROJECT_ID>`.`uk_flood_raw`.`stations` AS d WHERE EXISTS (SELECT 1 FROM `ukf` |
| runtime SA | SELECT | 102,760,448 | `SELECT DATE(datetime) d, COUNT(*) n, COUNT(*) - COUNT(DISTINCT CONCAT(measure, '\|', CAST(datetime AS STRING))) extra_dupes FROM `<GCP_PROJECT_ID>.uk_flood_raw.readings` WHERE dateti` |
| operator account | SELECT | 101,711,872 | `SELECT * FROM `<GCP_PROJECT_ID>.uk_flood_monitoring.raw_readings_duplicate_counts` ORDER BY reading_date` |
| runtime SA | SCRIPT | 83,886,080 | `begin transaction; delete from `<GCP_PROJECT_ID>`.`uk_flood`.`dbt_models` where metadata_hash is null or metadata_hash in (select metadata_hash from `<GCP_PROJECT_ID>`.`uk_flood`.`db` |
| runtime SA | SELECT | 83,886,080 | `SELECT `version`, `engine_version`, `pipeline_name`, `state`, `created_at`, `_dlt_load_id` FROM `<GCP_PROJECT_ID>`.`uk_flood_raw`.`_dlt_pipeline_state` AS s JOIN `<GCP_PROJECT_ID>`.`` |
| runtime SA | SELECT | 81,788,928 | `SELECT STRUCT<STRUCT<ARRAY<DATE>>>(( select as struct -- IGNORE NULLS: this needs to be aligned to _dbt_max_partition, which ignores null array_agg(distinct date(reading_date) IGNO` |
| runtime SA | SELECT | 69,206,016 | `select count(*) as failures, count(*) != 0 as should_warn, count(*) != 0 as should_error from ( -- PRD success criterion 2 (warehouse-side half): the natural key of a reading -- is` |
| runtime SA | DELETE | 41,943,040 | `delete from `<GCP_PROJECT_ID>`.`uk_flood`.`dbt_models` where metadata_hash is null or metadata_hash in (select metadata_hash from `<GCP_PROJECT_ID>`.`uk_flood`.`dbt_models__tmp_20261` |
| runtime SA | SCRIPT | 41,943,040 | `begin transaction; delete from `<GCP_PROJECT_ID>`.`uk_flood`.`dbt_tests` where metadata_hash is null or metadata_hash in (select metadata_hash from `<GCP_PROJECT_ID>`.`uk_flood`.`dbt` |
| runtime SA | SCRIPT | 41,943,040 | `DELETE FROM `<GCP_PROJECT_ID>`.`uk_flood_raw`.`measures` AS d WHERE EXISTS (SELECT 1 FROM `<GCP_PROJECT_ID>`.`uk_flood_raw_staging`.`measures` AS s WHERE d.`id` = s.`id`); INSERT INT` |
| runtime SA | SCRIPT | 41,943,040 | `DELETE FROM `<GCP_PROJECT_ID>`.`uk_flood_raw`.`flood_warnings` AS d WHERE EXISTS (SELECT 1 FROM `<GCP_PROJECT_ID>`.`uk_flood_raw_staging`.`flood_warnings` AS s WHERE d.`id` = s.`id`)` |
| runtime SA | SCRIPT | 41,943,040 | `DELETE FROM `<GCP_PROJECT_ID>`.`uk_flood_raw`.`readings_latest` AS d WHERE EXISTS (SELECT 1 FROM `<GCP_PROJECT_ID>`.`uk_flood_raw_staging`.`readings_latest` AS s WHERE d.`id` = s.`id` |
| runtime SA | INSERT | 41,943,040 | `INSERT INTO `<GCP_PROJECT_ID>`.`uk_flood_raw`.`readings`(`id`, `measure`, `datetime`, `value_raw`, `value_is_list`, `value_missing`, `qualifier`, `_resource`, `_dlt_load_id`, `_dlt_` |
| runtime SA | SELECT | 31,457,280 | `SELECT `version`, `engine_version`, `inserted_at`, `schema_name`, `version_hash`, `schema` FROM `<GCP_PROJECT_ID>`.`uk_flood_raw`.`_dlt_version` WHERE `version_hash` = ? LIMIT 1` |

## Top individual jobs

| Time (UTC) | Principal | Type | Billed | What it was |
|---|---|---|---:|---|
| 2026-11-16 17:13:28 | operator account | SELECT | 7,229,931,520 | SELECT 'readings' AS table_name, COUNT(*) AS row_count, COUNT(DISTINCT _dlt_id) AS distinct_dlt_ids, MIN(datetime) AS min_dt, MAX(datetime) AS max_dt, COUNTIF(measure IS NULL OR da |
| 2026-11-16 17:20:18 | runtime SA | CREATE_TABLE_AS_SELECT | 6,520,045,568 | dbt `acc93142-c771-4cb0-88b0-b597eedbcb0c`: create or replace table `<GCP_PROJECT_ID>`.`uk_flood_core`.`fct_readings__dbt_tmp` partition by reading_date cluster by measure_id OPTIONS( e |
| 2026-11-16 17:40:42 | runtime SA | SCRIPT | 3,919,577,088 | DELETE FROM `<GCP_PROJECT_ID>`.`uk_flood_raw`.`readings` AS d WHERE EXISTS (SELECT 1 FROM `<GCP_PROJECT_ID>`.`uk_flood_raw_staging`.`readings` AS s WHERE d.`measure` = s.`measure` AN |
| 2026-11-16 17:40:42 | runtime SA | DELETE | 3,898,605,568 | DELETE FROM `<GCP_PROJECT_ID>`.`uk_flood_raw`.`readings` AS d WHERE EXISTS (SELECT 1 FROM `<GCP_PROJECT_ID>`.`uk_flood_raw_staging`.`readings` AS s WHERE d.`measure` = s.`measure` AN |
| 2026-11-16 17:20:25 | runtime SA | SCRIPT | 3,436,183,552 | dbt `acc93142-c771-4cb0-88b0-b597eedbcb0c`: -- generated script to merge partitions into `<GCP_PROJECT_ID>`.`uk_flood_core`.`fct_readings` declare dbt_partitions_for_replacement array<d |
| 2026-11-16 17:40:12 | runtime SA | SCRIPT | 3,421,503,488 | DELETE FROM `<GCP_PROJECT_ID>`.`uk_flood_raw`.`readings` AS d WHERE EXISTS (SELECT 1 FROM `<GCP_PROJECT_ID>`.`uk_flood_raw_staging`.`readings` AS s WHERE d.`measure` = s.`measure` AN |
| 2026-11-16 17:40:12 | runtime SA | DELETE | 3,400,531,968 | DELETE FROM `<GCP_PROJECT_ID>`.`uk_flood_raw`.`readings` AS d WHERE EXISTS (SELECT 1 FROM `<GCP_PROJECT_ID>`.`uk_flood_raw_staging`.`readings` AS s WHERE d.`measure` = s.`measure` AN |
| 2026-11-16 17:20:27 | runtime SA | MERGE | 3,364,880,384 | dbt `acc93142-c771-4cb0-88b0-b597eedbcb0c`: merge into `<GCP_PROJECT_ID>`.`uk_flood_core`.`fct_readings` as DBT_INTERNAL_DEST using ( select * from `<GCP_PROJECT_ID>`.`uk_flood_core`.`fc |
| 2026-11-16 17:37:03 | runtime SA | SELECT | 1,209,008,128 | SELECT COUNT(*) n, MIN(datetime) min_dt, MAX(datetime) max_dt, COUNT(DISTINCT _dlt_id) distinct_ids, MAX(_dlt_load_id) max_load FROM `<GCP_PROJECT_ID>.uk_flood_raw.readings` |
| 2026-11-16 17:25:16 | runtime SA | SCRIPT | 501,219,328 | dbt `d8c0b709-071f-4885-b05b-07f1f6937ab4`: -- generated script to merge partitions into `<GCP_PROJECT_ID>`.`uk_flood_core`.`fct_readings` declare dbt_partitions_for_replacement array<d |
| 2026-11-16 17:25:17 | runtime SA | MERGE | 490,733,568 | dbt `d8c0b709-071f-4885-b05b-07f1f6937ab4`: merge into `<GCP_PROJECT_ID>`.`uk_flood_core`.`fct_readings` as DBT_INTERNAL_DEST using ( select * from `<GCP_PROJECT_ID>`.`uk_flood_core`.`fc |
| 2026-11-16 17:25:12 | runtime SA | CREATE_TABLE_AS_SELECT | 461,373,440 | dbt `d8c0b709-071f-4885-b05b-07f1f6937ab4`: create or replace table `<GCP_PROJECT_ID>`.`uk_flood_core`.`fct_readings__dbt_tmp` partition by reading_date cluster by measure_id OPTIONS( e |
| 2026-11-16 17:26:05 | operator account | SELECT | 330,301,440 | SELECT 'fct_readings' AS table_name, COUNT(*) AS row_count, MIN(reading_time) AS min_dt, MAX(reading_time) AS max_dt FROM `<GCP_PROJECT_ID>.uk_flood_core.fct_readings` UNION ALL SEL |
| 2026-11-16 17:12:20 | operator account | SELECT | 247,463,936 | SELECT 'readings_legacy_backup' AS source, COUNT(*) AS row_count, MIN(datetime) AS min_dt, MAX(datetime) AS max_dt FROM `<GCP_PROJECT_ID>.uk_flood_raw.readings_legacy_backup_check`  |
| 2026-11-16 17:27:29 | runtime SA | SELECT | 163,577,856 | dbt `da34fa4c-66b4-4185-93cf-349b5938b9eb`: select count(*) as failures, count(*) != 0 as should_warn, count(*) != 0 as should_error from ( -- Staging owns raw dedup: keep the latest _ |

## Findings

- The largest single consumer was a **7.23 GB diagnostic `SELECT`** by the operator account at 17:13 UTC that unioned full-table counts across `readings`, `readings_legacy`, and the archive backup checks.
- The largest dbt invocation (`acc93142-c771-4cb0-88b0-b597eedbcb0c`) billed **13.69 GB** across 37 jobs, including a 6.52 GB `fct_readings` temp-table build, a 3.36 GB partition MERGE, and 3.44 GB of generated script. This is consistent with an unbounded/very-wide `fct_readings` rebuild, not the intended 3-day lookback.
- Two `seed_history`-style DELETE+INSERT scripts at 17:40 UTC billed **3.92 GB** and **3.42 GB**; avoiding this merge path for the gap is the right call. The local archive can be appended with a free load job.
- A 1.21 GB full-table `COUNT(DISTINCT _dlt_id)` diagnostic and a 0.33 GB `fct_readings`/`stg_readings`/raw-7d count added another ~1.54 GB.
- The quota is project-wide. The runtime-SA-only view misses the operator-account jobs (7.95 GB here); grant the SA `bigquery.jobs.listAll` or use a separate monitoring principal to get a complete project view.

## Gap fill after the quota event

- **Method:** local archive -> filtered Parquet -> BigQuery load job (free, not a
  query and not `seed_history`). Boundary detected from
  `INFORMATION_SCHEMA.PARTITIONS`: the last complete raw day was 2026-11-10, so
  the filter started at 2026-11-10 22:55 UTC (1-hour overlap) and ended at the
  archive max 2026-11-17 00:08:35 UTC.
- **Dry run:** 2,960,892 rows, 5,348 measures, 0 duplicate `(measure, datetime)`
  keys, 0 null required fields, 69,333,461 B Parquet.
- **Load:** all 2,960,892 rows appended; load jobs do not consume
  QueryUsagePerDay.
- **Raw counts after load:** 11-10 485,807; 11-11 513,688; 11-12 506,797;
  11-13 506,646; 11-14 506,487; 11-15 506,154; 11-16 416,425; 11-17 318.
- **Coverage check:** no day is zero or below 80% of its archive count. The
  archive itself has 410,896 rows for 11-16 (83% of the 493,001 median), so raw
  matches the archive rather than a full 493k day.
- **Expected duplicates:** the 1-hour overlap on 11-10 and the old sparse
  latest-reading rows in the gap remain in raw; `stg_readings` dedups them.

## Widened-lookback dbt measurement (2026-11-17 01:21 UTC)

- **Command:** `dbt build --select fct_readings --vars '{"fct_lookback_days": 7}'`.
- **Result:** **4,779,409,408 B (~4.45 GiB) billed** across 44 billed query jobs
  (55 total including 0-byte DDL/transaction statements). Breakdown:
  fct `CREATE_TABLE_AS_SELECT` 1.20 GB, `MERGE` 1.20 GB, generated `SCRIPT` 1.23 GB,
  Elementary tests ~0.8 GB, dbt metadata inserts/deletes ~0.35 GB. The model reports
  1.1 GiB processed; billing is higher because the MERGE re-reads the fact table.
- **Interpretation:** this was a 7-partition gap rebuild (11-10..11-17), so it is a
  worst case, not the steady-state 3-day run. At this cost, 2-hourly dbt would be
  ~53 GiB/day, far above the 15 GiB/day rule.
- **Follow-up:** the immediate 3-day run failed with the same
  `Custom quota exceeded ... QueryUsagePerDay` error, so the steady-state 3-day
  measurement is still pending. Re-run after the 08:00 UTC reset.
