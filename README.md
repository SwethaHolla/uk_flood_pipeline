# UK Flood Pipeline

A resilient, observable batch ELT pipeline that ingests UK Environment Agency (EA) Flood Monitoring data, normalizes it through a medallion architecture, and delivers analytics-ready flood insights. Built with production-grade patterns for idempotency, data quality, resilience, and observability.

## What it does

- **Ingests** real-time and latest flood data (stations, measures, flood warnings, readings) from the [EA Flood Monitoring API](https://environment.data.gov.uk/flood-monitoring/doc/reference) with no API key required
- **Handles messy real-world data** - normalizes inconsistent types (list/numeric values), missing values, dirty categoricals, and timezone inconsistencies per EA API quirks
- **Loads to BigQuery** via [dlt](https://dlthub.com/) with quarantine for failed records, run metadata tracking, and schema change detection
- **Transforms with dbt** into a medallion model (staging → core → marts) with incremental processing, partitioning, and clustering
- **Orchestrates** with [Apache Airflow](https://airflow.apache.org/) on a 15-minute schedule (ingest → dbt → Elementary)
- **Monitors quality** with dbt tests, 4 singular data quality tests, and [Elementary](https://www.elementary-data.com/) anomaly detection
- **Validates resilience** with a chaos harness testing 7 failure scenarios (timeouts, 429s, 500s, malformed data, etc.)

## Architecture

```
EA Flood Monitoring API  →  Python (httpx) + Pydantic contracts  →  dlt  →  BigQuery (uk_flood_raw)
                                                                                    ↓
                                                                               dbt (staging/core/marts)
                                                                                    ↓
                                                                        marts (reliability/trends/warnings)
                                                                                    ↓
                                                                          Elementary + Airflow orchestration
```

**Local/offline parity:** Mock FastAPI server + DuckDB for development without GCP credentials. Airflow DAGs in `dags/`.

## Tech stack

| Layer | Tools |
| --- | --- |
| Language/runtime | Python 3.12, [uv](https://docs.astral.sh/uv/) |
| Ingestion | [dlt](https://dlthub.com/) 1.x (BigQuery/DuckDB), [httpx](https://www.python-httpx.org/), [tenacity](https://tenacity.readthedocs.io/) |
| Validation | [Pydantic v2](https://docs.pydantic.dev/) with strict contracts and quarantine |
| Transformation | [dbt-core 1.9.11](https://docs.getdbt.com/) + [dbt-bigquery 1.9.2](https://docs.getdbt.com/docs/core/connect-data-platform/bigquery-setup) / [dbt-duckdb 1.9.6](https://docs.getdbt.com/docs/core/connect-data-platform/duckdb-setup) |
| Data quality/observability | [Elementary 0.16.4](https://docs.elementary-data.com/) (BigQuery), dbt generic + singular tests |
| Orchestration | [Apache Airflow 2.10.4](https://airflow.apache.org/) via Docker Compose (LocalExecutor) |
| Testing/lint | [pytest](https://docs.pytest.org/), [ruff](https://docs.astral.sh/ruff/), [sqlfluff](https://sqlfluff.com/), [pre-commit](https://pre-commit.com/) |
| CI/CD | GitHub Actions |

## Results & Metrics

### Data Quality (BigQuery - `ukfloodpipeline`)
| Metric | Result |
| --- | --- |
| dbt build (dev target) | **85 PASS / 1 WARN / 0 ERR** (86 total) |
| Models built | 16 incremental + 27 views + 1 table |
| Data tests | 40 schema tests + 4 singular tests |
| Elementary anomaly monitors | **Passed** (ran on marts) |
| Raw data loaded | 5,404+ latest readings, 5,538 stations, 7,429 measures, 7 flood warnings |

### Data Quality (Local/DuckDB)
| Metric | Result |
| --- | --- |
| dbt build (local target) | **52 PASS / 0 WARN / 0 ERR** |
| Offline tests | **38 passed** (100%) |
| Linting (ruff + sqlfluff) | **Clean** |

### Performance (BigQuery) — **UNVERIFIED / superseded**

> These numbers are retained for reference only and are **not evidence**. They were
> produced by a single un-repeated run, every query was billed at BigQuery's 10 MB
> minimum-per-table floor, and the rate used ($5/TB) was wrong. Official on-demand
> pricing is **$6.25 per TiB** (first 1 TiB/month free, 10 MB minimum per table +
> query). The rigorous, repeatable benchmark is regenerated in Phase F and will
> report N>=5 runs, median + p95, dry-run byte pruning, and `INFORMATION_SCHEMA.JOBS`
> slot usage.

| Query | Time | Bytes Billed | Est. Cost |
| --- | --- | --- | --- |
| Daily readings (7 days) | 1.83s | 10.49 MB | $0.000048 |
| Station completeness (30 days) | 1.23s | 10.49 MB | $0.000048 |
| Active flood warnings | 1.21s | 10.49 MB | $0.000048 |
| River level trends (24h) | 1.32s | 41.94 MB | $0.000191 |
| **Total** | **5.59s** | **73.40 MB** | **$0.000334** |

*Optimizations:* `fct_readings` partitioned by `reading_date` (DAY), clustered by `measure_id`, incremental (MERGE on BigQuery / DELETE+INSERT on DuckDB).

### Resilience (Chaos Testing) — **UNVERIFIED / superseded**

> This table is **not evidence**: the harness that produced it recorded a connection
> refusal as "Handled" (a reporting bug, now fixed in Phase B) and the scenarios were
> run offline. Chaos v2 (schema drift, type change, duplicates, late/out-of-order,
> empty, partial payload, quarantine flood) is regenerated in Phase E.

| Scenario | Expected Behavior | Status |
| --- | --- | --- |
| API Timeout | Retry with exponential backoff | **Handled** |
| 429 Rate Limit | Respect retry-after, backoff | **Handled** |
| 500 Server Error | Retry with backoff | **Handled** |
| Malformed JSON | Parse error → quarantine | **Handled** |
| Missing/Bad Fields | Contract validation → quarantine | **Handled** |
| Network Error | Connection retry with backoff | **Handled** |
| Slow Response | Timeout → retry/backoff | **Handled** |

All failures routed to `quarantine` table with `raw_payload` and `error_reason`. Pipeline continues on individual resource failures.

## Quick start (offline - no GCP needed)

```bash
# Install dependencies
uv sync --all-extras --dev

# Ingest mock EA API data into local DuckDB
make ingest-local

# Build and test dbt models against DuckDB
make dbt-build-local

# Run offline tests
make test
```

## GCP / BigQuery setup

```bash
# Authenticate
gcloud auth login
gcloud auth application-default login

# Set environment
export GCP_PROJECT_ID=ukfloodpipeline
export BQ_LOCATION=europe-west2
export BQ_RAW_DATASET=uk_flood_raw
export BQ_DBT_SCHEMA=uk_flood
# Auth: use ADC or service-account impersonation — no downloaded key files.
#   gcloud auth application-default login
#   (optional) export GCP_IMPERSONATE_SA=dlt-runner@ukfloodpipeline.iam.gserviceaccount.com

# Ingest from EA API
make ingest

# Seed ~4 weeks of history (per-measure)
make seed-history

# Transform with dbt
make dbt-deps
make dbt-build

# Generate Elementary report
make elementary-report
```

## Orchestration (Airflow)

```bash
# Start Airflow locally
cd docker
docker compose up -d

# Access UI at http://localhost:8080 (admin/admin)
# DAG: uk_flood_pipeline (runs every 15 min)
# Tasks: ingest_raw_data → dbt_build → elementary_monitoring
```

## Project structure

```text
src/uk_flood/        # contracts, EA client, dlt pipelines, settings, CLI
mock_api/            # FastAPI mock EA API + fixtures (mirrors real API quirks)
scripts/             # ingest_local, seed_history, chaos, benchmark
dags/                # Airflow DAGs
docker/              # Airflow Dockerfile + docker-compose
dbt/                 # staging/core/marts, macros, tests, profiles, packages
tests/               # offline pytest suite (38 tests)
.github/workflows/   # CI (lint, test, dbt parse, sqlfluff, fresh-clone)
docs/                # PRD, DECISIONS, NEXT_STEPS, chaos-results, benchmarks
```

## Data model

**Raw (dlt):** `stations`, `measures`, `flood_warnings`, `readings_latest`, `readings` (history), `quarantine`, `run_metadata`, `stations__measures` (normalized)

**Staging:** Type-safe views handling EA quirks - list values → `value_raw`/`value_is_list`, missing values → `value_missing`, dirty categoricals cleaned via `clean_categorical()` macro, naive timestamps normalized to UTC

**Core:** `dim_station`, `dim_measure`, `dim_flood_area`, `fct_readings` (incremental, partitioned/clustered), `fct_flood_warnings`

**Marts:** `mart_station_reliability` (completeness/freshness), `mart_river_level_trends`, `mart_rainfall_trends`, `mart_flood_warning_history`

**Data quality:** Schema tests (unique/not_null/relationships/accepted_values) + singular tests (`assert_reading_time_plausible`, `assert_no_duplicate_reading_keys`, `assert_list_values_always_have_raw`, `assert_bridge_measures_resolve`)

## Key features

- **Idempotent ingestion** - dlt merge keys, run metadata tracking, safe to re-run
- **Resilient client** - Retries with exponential backoff, timeouts, handles 429/5xx, follows redirects
- **Quarantine system** - All contract validation failures captured with full raw payload
- **Smart incremental** - dbt incremental with adapter-specific strategies (MERGE on BigQuery, DELETE+INSERT on DuckDB)
- **Cost-aware** - Partitioning/clustering, `maximum_bytes_billed`, minimal scan costs
- **Offline-first** - Full local development with mock API and DuckDB
- **CI/CD** - Lint, test, dbt parse, sqlfluff, fresh-clone checks on every PR

## Repository

[https://github.com/SwethaHolla/uk_flood_pipeline](https://github.com/SwethaHolla/uk_flood_pipeline)

## License

MIT
