"""UK Flood Pipeline orchestration DAG.

Runs on a 15-minute schedule:
1. Ingest latest readings/warnings/stations/measures from EA API -> BigQuery raw
2. Run dbt build (staging/core/marts + tests)
3. Run Elementary data quality checks
4. Send alerts on failure (configurable)
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DEFAULT_ARGS = {
    "owner": "uk-flood-pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

PROJECT_DIR = "/opt/airflow"
DBT_DIR = os.path.join(PROJECT_DIR, "dbt")
PYTHONPATH = "/opt/airflow/src"

with DAG(
    dag_id="uk_flood_pipeline",
    default_args=DEFAULT_ARGS,
    description="Ingest EA flood data -> dlt/BigQuery -> dbt -> Elementary",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["uk-flood", "ingestion", "dbt"],
) as dag:
    ingest_raw = BashOperator(
        task_id="ingest_raw_data",
        bash_command=f"cd {PROJECT_DIR} && PYTHONPATH={PYTHONPATH} python -m uk_flood.ingest run --destination bigquery",
        env={
            "GCP_PROJECT_ID": os.environ.get("GCP_PROJECT_ID", "ukfloodpipeline"),
            "BQ_RAW_DATASET": os.environ.get("BQ_RAW_DATASET", "uk_flood_raw"),
            "BQ_LOCATION": os.environ.get("BQ_LOCATION", "europe-west2"),
            "GOOGLE_APPLICATION_CREDENTIALS": os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""),
            "GOOGLE_CLOUD_PROJECT": os.environ.get("GCP_PROJECT_ID", "ukfloodpipeline"),
        },
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && PYTHONPATH={PYTHONPATH} dbt deps --profiles-dir . --project-dir . && dbt build --profiles-dir . --project-dir . --target dev",
        env={
            "GCP_PROJECT_ID": os.environ.get("GCP_PROJECT_ID", "ukfloodpipeline"),
            "BQ_DBT_SCHEMA": os.environ.get("BQ_DBT_SCHEMA", "uk_flood"),
            "BQ_LOCATION": os.environ.get("BQ_LOCATION", "europe-west2"),
            "GOOGLE_APPLICATION_CREDENTIALS": os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""),
            "DBT_PROFILES_DIR": DBT_DIR,
        },
    )

    elementary_report = BashOperator(
        task_id="elementary_monitoring",
        bash_command=f"cd {DBT_DIR} && PYTHONPATH={PYTHONPATH} elementary monitor --profiles-dir . --project-dir . || true",
        env={
            "GCP_PROJECT_ID": os.environ.get("GCP_PROJECT_ID", "ukfloodpipeline"),
            "BQ_DBT_SCHEMA": os.environ.get("BQ_DBT_SCHEMA", "uk_flood"),
            "BQ_LOCATION": os.environ.get("BQ_LOCATION", "europe-west2"),
            "GOOGLE_APPLICATION_CREDENTIALS": os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""),
        },
    )

    ingest_raw >> dbt_build >> elementary_report
