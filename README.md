# UK Flood Pipeline

An end-to-end data engineering portfolio project: ingest Environment Agency (EA) flood-monitoring data, load it to BigQuery via [dlt](https://dlthub.com/), transform with [dbt](https://www.getdbt.com/), orchestrate with [Apache Airflow](https://airflow.apache.org/) (Docker), monitor with [Elementary](https://www.elementary-data.com/), and gate quality with [pytest](https://docs.pytest.org/), [ruff](https://docs.astral.sh/ruff/), [sqlfluff](https://sqlfluff.com/), and GitHub Actions.

## Architecture

```
EA Flood Monitoring API  →  Python (httpx) + Pydantic contracts  →  dlt  →  BigQuery (raw: uk_flood_raw)
                                                                                    ↓
                                                                               dbt (staging/core/marts)
                                                                                    ↓
                                                                           marts (reliability/trends/warnings)
```

Local/offline parity uses a mock FastAPI server + DuckDB. Airflow DAGs live under `dags/`.

## Tech stack

| Layer | Tool |
| --- | --- |
| Language/runtime | Python 3.12, [uv](https://docs.astral.sh/uv/) |
| Ingestion | [dlt](https://dlthub.com/) (BigQuery/DuckDB) |
| Validation | [Pydantic v2](https://docs.pydantic.dev/) |
| Transformation | [dbt-core 1.9](https://docs.getdbt.com/) + [dbt-bigquery](https://docs.getdbt.com/docs/core/connect-data-platform/bigquery-setup) / [dbt-duckdb](https://docs.getdbt.com/docs/core/connect-data-platform/duckdb-setup) |
| Data quality/observability | [Elementary 0.16](https://docs.elementary-data.com/) (BigQuery only), dbt tests + 4 singular tests |
| Orchestration | [Apache Airflow](https://airflow.apache.org/) via `docker/docker-compose.yml` |
| Testing/lint | [pytest](https://docs.pytest.org/), [ruff](https://docs.astral.sh/ruff/), [sqlfluff](https://sqlfluff.com/), [pre-commit](https://pre-commit.com/) |
| CI | GitHub Actions (`.github/workflows/ci.yml`) |

## Quick start (offline)

```bash
# Install dependencies
uv sync --all-extras --dev

# Ingest mock API into local DuckDB
make ingest-local

# Build dbt models/tests against DuckDB
make dbt-build-local
```

## GCP / BigQuery

```bash
# Authenticate (service account key or ADC)
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
export GCP_PROJECT_ID=ukfloodpipeline
export BQ_LOCATION=US
export BQ_RAW_DATASET=uk_flood_raw
export BQ_DBT_SCHEMA=uk_flood

# Run one ingestion cycle against EA API
make ingest

# Seed ~4 weeks of per-measure readings history
make seed-history

# Transform in BigQuery
make dbt-deps
make dbt-build
```

## Project layout

```text
src/uk_flood/        # contracts, EA client, dlt pipelines, settings
mock_api/            # FastAPI mock EA API + fixtures (quirk-embedded)
scripts/             # ingest_local, seed_history, chaos, benchmark
dbt/                 # staging/core/marts, macros, tests, profiles/packages
dags/                # Airflow DAGs
docker/              # Airflow docker-compose
tests/               # offline pytest suite (38 tests)
.github/workflows/   # CI (lint, test, dbt parse, sqlfluff, fresh-clone)
docs/                # PRD, decisions, next steps
```

## Data model (dbt)

- **Staging** (`stg_*`): typed/renamed, cleans PRD §7 quirks — list readings (`value_raw`/`value_is_list`), missing values (`value_missing`), dirty categoricals via `clean_categorical()`, naive timestamps treated as UTC, dlt array normalisation handled (`stations__measures`).
- **Core**: `dim_station`, `dim_measure`, `dim_flood_area`, `fct_readings` (incremental — `merge`+partition/cluster on BigQuery, `delete+insert` on DuckDB), `fct_flood_warnings`.
- **Marts**: `mart_station_reliability` (completeness/freshness), `mart_river_level_trends` / `mart_rainfall_trends` (hourly), `mart_flood_warning_history`.
- **Tests**: schema tests + singular tests (`assert_no_future_readings`, `assert_no_duplicate_reading_keys`, `assert_list_values_always_have_raw`, `assert_bridge_measures_resolve`). Elementary anomaly monitors are gated to BigQuery.

## Observability & hygiene

- **Quarantine**: contract failures captured with raw payload + error reason.
- **Run metadata**: one row per resource per ingestion run.
- **Idempotent loads**: dlt merge keys + `run_metadata` schema hashing for change detection.
- **Cost guardrails**: `maximum_bytes_billed` on BigQuery, `.env` secrets never committed.

## Verification (current state)

- **BigQuery**: `uk_flood_raw` loaded via dlt; `dbt build --target dev` — 85/86 PASS (1 expected warning).
- **Local/DuckDB**: `dbt build --target local` — 52/52 PASS; `pytest -m "not integration"` — 38 passed.
- **CI**: lint/test/dbt-parse/sqlfluff/fresh-clone all green on `main`.

## License

MIT
