from datetime import UTC, datetime

from uk_flood.dag_schedule import interval_end, should_build_dbt, should_ingest_reference


def _at(hour: int, minute: int) -> datetime:
    return datetime(2026, 11, 16, hour, minute, tzinfo=UTC)


def test_reference_ingest_runs_only_at_0200_utc():
    assert should_ingest_reference({"data_interval_end": _at(2, 0)}) is True
    assert should_ingest_reference({"data_interval_end": _at(2, 15)}) is False
    assert should_ingest_reference({"data_interval_end": _at(3, 0)}) is False


def test_reference_ingest_falls_back_to_logical_date():
    assert should_ingest_reference({"logical_date": _at(2, 0)}) is True
    assert should_ingest_reference({}) is False


def test_dbt_runs_every_two_hours_at_minute_zero():
    assert should_build_dbt({"data_interval_end": _at(0, 0)}) is True
    assert should_build_dbt({"data_interval_end": _at(2, 0)}) is True
    assert should_build_dbt({"data_interval_end": _at(22, 0)}) is True
    assert should_build_dbt({"data_interval_end": _at(1, 0)}) is False
    assert should_build_dbt({"data_interval_end": _at(2, 15)}) is False
    assert should_build_dbt({}) is False


def test_interval_end_prefers_data_interval_end():
    end = _at(4, 0)
    logical_date = _at(3, 0)
    assert interval_end({"data_interval_end": end, "logical_date": logical_date}) is end


def test_dbt_cadence_is_configurable():
    assert should_build_dbt({"data_interval_end": _at(4, 0)}, interval_hours=4) is True
    assert should_build_dbt({"data_interval_end": _at(2, 0)}, interval_hours=4) is False
    assert should_build_dbt({"data_interval_end": _at(6, 0)}, interval_hours=6) is True
