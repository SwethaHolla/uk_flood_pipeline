"""Pure schedule gates shared by the Airflow DAG and offline tests."""

from __future__ import annotations

from datetime import datetime


def interval_end(context: dict) -> datetime | None:
    return context.get("data_interval_end") or context.get("logical_date")


def should_ingest_reference(context: dict) -> bool:
    end = interval_end(context)
    return end is not None and end.hour == 2 and end.minute == 0


def should_build_dbt(context: dict, interval_hours: int = 2) -> bool:
    if interval_hours < 1:
        raise ValueError("interval_hours must be >= 1")
    end = interval_end(context)
    return end is not None and end.minute == 0 and end.hour % interval_hours == 0
