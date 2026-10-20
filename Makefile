SHELL := bash
.DEFAULT_GOAL := help

UV := uv
PYTHON := $(UV) run python

.PHONY: help setup sync up down restart logs ingest seed-history dbt-deps dbt-build dbt-test \
        elementary-report test coverage lint fmt sqlfluff pre-commit-install pre-commit ci chaos benchmark clean

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup: ## Install dependencies (uv + Python 3.12)
	$(UV) python pin 3.12
	$(UV) sync --all-extras --dev

sync: ## Re-sync dependencies from lockfile
	$(UV) sync --all-extras --dev

up: ## Start Airflow (scheduler, webserver, postgres) in the background
	docker compose -f docker/docker-compose.yml up -d --build

down: ## Stop Airflow
	docker compose -f docker/docker-compose.yml down

restart: down up ## Restart Airflow

logs: ## Tail Airflow logs
	docker compose -f docker/docker-compose.yml logs -f

ingest: ## Run one ingestion cycle against the EA API (needs BigQuery)
	$(PYTHON) -m uk_flood.ingest run

seed-history: ## Seed ~4 weeks of readings history via ?date= loops (needs BigQuery)
	$(PYTHON) scripts/seed_history.py

dbt-deps: ## Install dbt packages
	cd dbt && $(UV) run dbt deps

dbt-build: ## Run the full dbt build (models + tests) (needs BigQuery)
	cd dbt && $(UV) run dbt build --profiles-dir . --project-dir .

dbt-test: ## Run dbt tests only
	cd dbt && $(UV) run dbt test --profiles-dir . --project-dir .

elementary-report: ## Generate the Elementary HTML report
	cd dbt && $(UV) run elementary generate-report --profiles-dir . --project-dir .

test: ## Run the offline test suite (no cloud credentials required)
	$(UV) run pytest -m "not integration"

coverage: ## Run offline tests with coverage report
	$(UV) run pytest -m "not integration" --cov=src --cov-report=term-missing

lint: ## Lint Python (ruff) and SQL (sqlfluff)
	$(UV) run ruff check src tests scripts mock_api dags
	$(UV) run ruff format --check src tests scripts mock_api dags
	$(UV) run sqlfluff lint dbt/models

fmt: ## Auto-format Python and fix ruff findings
	$(UV) run ruff check --fix src tests scripts mock_api dags
	$(UV) run ruff format src tests scripts mock_api dags

sqlfluff: ## Lint dbt SQL only
	$(UV) run sqlfluff lint dbt/models

pre-commit-install: ## Install git pre-commit hooks
	$(UV) run pre-commit install

pre-commit: ## Run all pre-commit hooks against every file
	$(UV) run pre-commit run --all-files

ci: lint test ## Approximate the CI gate locally

chaos: ## Run the chaos harness against the local mock API
	$(UV) run python scripts/chaos.py

benchmark: ## Run the before/after BigQuery benchmark (needs BigQuery)
	$(UV) run python scripts/benchmark.py

clean: ## Remove build/test artefacts
	rm -rf .pytest_cache .ruff_cache htmlcov .coverage dbt/target dbt/dbt_packages dbt/logs
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
