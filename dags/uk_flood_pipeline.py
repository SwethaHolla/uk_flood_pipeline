"""UK Flood Pipeline orchestration DAG.

Runs every 15 minutes with cost-gated work:
1. Ingest latest readings from the EA API -> BigQuery raw (every 15 min)
2. Refresh reference data (stations/measures/warnings) daily at 02:00 UTC
3. Run dbt build (staging/core/marts + tests) every DBT_INTERVAL_HOURS
   (default 2 hours)
4. Run Elementary data quality checks after dbt
5. Record ShortCircuit decisions in uk_flood_ops.gate_events and optionally
   alert on trips (off by default)

A full ingest cycle bills ~580 MiB because dlt rewrites the reference tables,
so reference data is refreshed daily and dbt runs every DBT_INTERVAL_HOURS to stay under
the project's BigQuery QueryUsagePerDay quota.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator

from uk_flood.dag_schedule import should_build_dbt, should_ingest_reference
from uk_flood.ops import evaluate_gate

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
GATE_FAIL_MODE = os.environ.get("OPS_GATE_FAIL_MODE", "closed")
DBT_INTERVAL_HOURS = int(os.environ.get("DBT_INTERVAL_HOURS", "2"))


def _gate_reference(**context: Any) -> bool:
    return evaluate_gate("reference", context, should_ingest_reference, GATE_FAIL_MODE)


def _gate_dbt(**context: Any) -> bool:
    return evaluate_gate(
        "dbt",
        context,
        lambda ctx: should_build_dbt(ctx, DBT_INTERVAL_HOURS),
        GATE_FAIL_MODE,
    )


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
    ingest_env = {
        "GCP_PROJECT_ID": os.environ.get("GCP_PROJECT_ID", "<GCP_PROJECT_ID>"),
        "BQ_RAW_DATASET": os.environ.get("BQ_RAW_DATASET", "uk_flood_raw"),
        "BQ_LOCATION": os.environ.get("BQ_LOCATION", "europe-west2"),
        "GOOGLE_APPLICATION_CREDENTIALS": os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""),
        "GOOGLE_CLOUD_PROJECT": os.environ.get("GCP_PROJECT_ID", "<GCP_PROJECT_ID>"),
    }

    ingest_readings = BashOperator(
        task_id="ingest_readings",
        bash_command=(
            f"cd {PROJECT_DIR} && PYTHONPATH={PYTHONPATH} "
            "python -m uk_flood.ingest run --destination bigquery "
            "--resources readings_latest"
        ),
        env=ingest_env,
    )

    gate_reference = ShortCircuitOperator(
        task_id="should_ingest_reference",
        python_callable=_gate_reference,
    )

    ingest_reference = BashOperator(
        task_id="ingest_reference",
        bash_command=(
            f"cd {PROJECT_DIR} && PYTHONPATH={PYTHONPATH} "
            "python -m uk_flood.ingest run --destination bigquery "
            "--resources stations,measures,flood_warnings"
        ),
        env=ingest_env,
    )

    gate_dbt = ShortCircuitOperator(
        task_id="should_build_dbt",
        python_callable=_gate_dbt,
        trigger_rule="none_failed_min_one_success",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && PYTHONPATH={PYTHONPATH} dbt deps --profiles-dir . --project-dir . && dbt build --profiles-dir . --project-dir . --target dev",
        env={
            "GCP_PROJECT_ID": os.environ.get("GCP_PROJECT_ID", "<GCP_PROJECT_ID>"),
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
            "GCP_PROJECT_ID": os.environ.get("GCP_PROJECT_ID", "<GCP_PROJECT_ID>"),
            "BQ_DBT_SCHEMA": os.environ.get("BQ_DBT_SCHEMA", "uk_flood"),
            "BQ_LOCATION": os.environ.get("BQ_LOCATION", "europe-west2"),
            "GOOGLE_APPLICATION_CREDENTIALS": os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""),
        },
    )

    ingest_readings >> gate_reference >> ingest_reference >> gate_dbt
    ingest_readings >> gate_dbt
    gate_dbt >> dbt_build >> elementary_report
