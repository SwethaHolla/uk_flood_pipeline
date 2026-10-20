# UK Flood Pipeline

Resilient, observable batch ELT pipeline over the [UK Environment Agency Flood
Monitoring API](https://environment.data.gov.uk/flood-monitoring/documentation/)
(beta, no API key, Open Government Licence v3).

> Status: under active build. See [docs/PRD.md](docs/PRD.md) for the full
> product requirements and delivery plan.

## Stack

**Python 3.12 · dlt · Pydantic · BigQuery · dbt-core · Apache Airflow ·
Elementary · pytest · Docker · GitHub Actions**

## Quickstart

```bash
make setup   # install dependencies (uv, Python 3.12)
make test    # run the offline test suite
make up      # start Airflow (Docker)
```

Full quickstart (BigQuery required): _coming with Phase 7._

## Documentation

| Doc | Purpose |
|---|---|
| [docs/PRD.md](docs/PRD.md) | Product requirements, API findings, architecture, plan |
| [docs/DECISIONS.md](docs/DECISIONS.md) | Decision log |
| [docs/NEXT_STEPS.md](docs/NEXT_STEPS.md) | Open threads / blockers |

## Data source attribution

Contains public sector information licensed under the Open Government
Licence v3.0. Source: UK Environment Agency Flood Monitoring API.
