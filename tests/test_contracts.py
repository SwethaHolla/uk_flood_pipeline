"""Contract tests: every documented API quirk (PRD §7) has a home here."""

from __future__ import annotations

from datetime import UTC

import pytest
from pydantic import ValidationError

from uk_flood.contracts import (
    FloodWarningContract,
    MeasureContract,
    ReadingContract,
    StationContract,
)


class TestReadingQuirks:
    def test_normal_float_reading(self):
        r = ReadingContract.model_validate(
            {
                "@id": ".../readings/1",
                "measure": ".../measures/1029TH-level-15-min",
                "dateTime": "2026-10-20T09:00:00Z",
                "value": 0.842,
            }
        )
        assert r.value_raw == 0.842
        assert not r.value_is_list
        assert not r.value_missing
        assert r.datetime.tzinfo is not None

    def test_quirk_1_list_value_kept_with_flag(self):
        """Observed `"value": [0.0, 0.0]` — head kept, flag raised, never crashes."""
        r = ReadingContract.model_validate(
            {
                "@id": ".../readings/2",
                "measure": ".../measures/1106TD-rainfall",
                "dateTime": "2026-10-20T09:00:00Z",
                "value": [0.0, 0.0],
            }
        )
        assert r.value_raw == 0.0
        assert r.value_is_list is True
        assert r.value_missing is False

    def test_quirk_2_missing_value_is_none(self):
        r = ReadingContract.model_validate(
            {
                "@id": ".../readings/3",
                "measure": ".../measures/x",
                "dateTime": "2026-10-20T09:00:00Z",
            }
        )
        assert r.value_raw is None
        assert r.value_missing is True

    def test_quirk_6_zulu_timestamp_is_utc(self):
        r = ReadingContract.model_validate(
            {
                "@id": ".../readings/4",
                "measure": ".../measures/x",
                "dateTime": "2026-10-20T09:00:00Z",
                "value": 1.0,
            }
        )
        assert r.datetime.tzinfo is not None
        assert r.datetime.utcoffset() == UTC.utcoffset(None)

    def test_missing_measure_is_quarantined(self):
        with pytest.raises(ValidationError):
            ReadingContract.model_validate(
                {"@id": ".../readings/5", "dateTime": "2026-10-20T09:00:00Z"}
            )

    def test_missing_id_is_quarantined(self):
        with pytest.raises(ValidationError):
            ReadingContract.model_validate({"measure": ".../m", "dateTime": "2026-10-20T09:00:00Z"})


class TestStationQuirks:
    def test_quirk_3_list_coordinates_coerced(self):
        s = StationContract.model_validate(
            {"@id": ".../stations/3421TH", "lat": [51.4801], "easting": [498230.0]}
        )
        assert s.lat == 51.4801
        assert s.easting == 498230.0

    def test_null_coordinates_stay_null(self):
        s = StationContract.model_validate({"@id": ".../stations/5301TH", "lat": None})
        assert s.lat is None

    def test_quirk_4_sparse_fields_optional(self):
        s = StationContract.model_validate({"@id": ".../stations/x"})
        assert s.town is None
        assert s.measures == []

    def test_measures_array_flattened_to_ids(self):
        s = StationContract.model_validate(
            {
                "@id": ".../stations/1",
                "measures": [{"@id": ".../measures/a"}, ".../measures/b"],
            }
        )
        assert s.measures == [".../measures/a", ".../measures/b"]

    def test_non_numeric_coordinate_quarantined(self):
        with pytest.raises(ValidationError):
            StationContract.model_validate({"@id": ".../stations/bad", "lat": "not-a-number"})

    def test_rloi_id_string_coerced_to_int(self):
        s = StationContract.model_validate({"@id": ".../stations/1", "RLOIid": "2034"})
        assert s.rloi_id == 2034


class TestMeasureQuirks:
    def test_latest_reading_flattened(self):
        m = MeasureContract.model_validate(
            {
                "@id": ".../measures/1029TH-level-15-min",
                "unitName": "m",
                "latestReading": {"dateTime": "2026-10-20T09:00:00Z", "value": 0.842},
            }
        )
        assert m.latest_reading_value == 0.842
        assert m.latest_reading_datetime is not None

    def test_list_latest_value_flagged(self):
        m = MeasureContract.model_validate(
            {
                "@id": ".../measures/rain",
                "latestReading": {"dateTime": "2026-10-20T09:00:00Z", "value": [0.0, 0.0]},
            }
        )
        assert m.latest_reading_is_list is True
        assert m.latest_reading_value == 0.0

    def test_no_latest_reading_flagged(self):
        m = MeasureContract.model_validate({"@id": ".../measures/x"})
        assert m.latest_reading_missing is True
        assert m.latest_reading_value is None

    def test_dirty_categoricals_kept_raw(self):
        """Quirk 8: '---'/'None' are preserved raw; dbt staging cleans them."""
        m = MeasureContract.model_validate({"@id": ".../measures/x", "unitName": "---"})
        assert m.unit_name == "---"


class TestFloodWarningQuirks:
    def test_quirk_6_naive_timestamp_assumed_utc(self):
        f = FloodWarningContract.model_validate(
            {
                "@id": ".../floods/1",
                "timeRaised": "2026-10-19T16:34:06",
                "severityLevel": 4,
            }
        )
        assert f.time_raised is not None
        assert f.time_raised.tzinfo is not None
        assert f.time_raised.utcoffset() == UTC.utcoffset(None)

    def test_nested_flood_area_flattened(self):
        f = FloodWarningContract.model_validate(
            {
                "@id": ".../floods/1",
                "floodArea": {"@id": ".../flood-area/124963", "label": "Kingston"},
            }
        )
        assert f.flood_area_id == ".../flood-area/124963"
        assert f.flood_area_label == "Kingston"

    def test_extra_fields_ignored(self):
        f = FloodWarningContract.model_validate(
            {"@id": ".../floods/1", "someFutureField": {"nested": True}}
        )
        assert f.id == ".../floods/1"
