"""Active-measure selection tests (seed history).

The EA API has no status field. We verified empirically that measures with a null
``latestReading`` return 0 rows while stale measures still return partial history,
so ``active_only`` must keep stale-but-present measures and skip only null ones.
"""

from __future__ import annotations

from scripts.seed_history import _select_measures

REF = "2026-10-25T18:00:00Z"


def _measures() -> list[dict]:
    return [
        {"@id": "m/fresh", "latestReading": {"dateTime": "2026-10-25T17:45:00Z"}, "period": 900},
        {"@id": "m/stale", "latestReading": {"dateTime": "2026-10-01T00:00:00Z"}, "period": 900},
        {"@id": "m/none", "latestReading": None, "period": 900},
        {"@id": "m/hourly", "latestReading": {"dateTime": "2026-10-25T17:00:00Z"}, "period": 3600},
        {
            "@id": "m/noperiod",
            "latestReading": {"dateTime": "2026-10-25T17:00:00Z"},
            "period": None,
        },
    ]


def test_active_only_keeps_stale_and_skips_null():
    selected, stats = _select_measures(_measures(), None, active_only=True, active_within_days=0)
    assert "m/fresh" in selected
    assert "m/stale" in selected, "stale measures still return partial history"
    assert "m/none" not in selected, "null latestReading returns 0 rows"
    assert stats["skipped_inactive"] == 1
    assert stats["reference"] == "2026-10-25T17:45:00+00:00"


def test_active_within_days_is_stricter():
    selected, _ = _select_measures(_measures(), None, active_only=True, active_within_days=2)
    assert "m/stale" not in selected
    assert "m/fresh" in selected


def test_est_rows_per_day_from_period():
    selected, stats = _select_measures(_measures(), None, active_only=False, active_within_days=0)
    # 900s -> 96/day, 900s -> 96, 900s -> 96, 3600s -> 24, null period -> 0
    assert stats["selected"] == len(selected) == 5
    assert stats["est_rows_day"] == 96 + 96 + 96 + 24


def test_measure_limit_applies_after_filter():
    selected, stats = _select_measures(_measures(), 2, active_only=True, active_within_days=0)
    assert len(selected) == 2
    assert stats["selected"] == 2
