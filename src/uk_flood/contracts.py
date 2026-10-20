"""Pydantic contracts for EA Flood Monitoring API payloads.

Encoding the documented API quirks (see docs/PRD.md §7):

1. Reading ``value`` can be a list (observed ``[0.0, 0.0]``) — kept as
   ``value_raw`` (head of list) + ``value_is_list`` flag so the column stays
   numeric and loadable while the anomaly remains visible/queryable.
2. Reading ``value`` can be absent (NaN is illegal JSON) — ``value_missing``.
3. Station ``lat``/``easting`` observed as list or null — lists are coerced to
   their first element (rare: 1 in ~5500), nulls stay null.
4. Many sparse optional fields — everything optional except natural keys.
5. Nested structures are flattened deliberately (``latestReading``,
   ``floodArea``, station ``measures[]`` kept as id list).
6. Timezone inconsistency: flood timestamps are naive; readings end in ``Z`` —
   all datetimes are validated to tz-aware UTC (naive treated as UTC).
12. ``@id`` is the natural key everywhere; readings additionally keyed by
    ``(measure, dateTime)`` for idempotency.
"""

from __future__ import annotations

import datetime as _dt
from datetime import UTC
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator


def _coerce_numeric(value: Any) -> float | None:
    """Coerce observed coordinate/value anomalies: list -> first element."""
    if value is None:
        return None
    if isinstance(value, list):
        return float(value[0]) if value else None
    return float(value)


def _ensure_utc(dt: _dt.datetime | None) -> _dt.datetime | None:
    """Treat naive timestamps as UTC (flood warnings) and normalise to UTC."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


class _BaseContract(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")


class LatestReadingContract(_BaseContract):
    """Nested ``latestReading`` on a measure — flattened to scalar columns."""

    measure: str | None = None
    datetime: _dt.datetime | None = None
    value_raw: float | None = None
    value_is_list: bool = False
    value_missing: bool = False

    @model_validator(mode="before")
    @classmethod
    def _flatten(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        out = dict(data)
        raw = out.pop("value", None)
        if raw is None and "value" in data:
            out["value_missing"] = True
        elif isinstance(raw, list):
            out["value_raw"] = float(raw[0]) if raw else None
            out["value_is_list"] = True
        elif raw is not None:
            out["value_raw"] = float(raw)
        else:
            out["value_missing"] = True
        if "dateTime" in out:
            out["datetime"] = out.pop("dateTime")
        return out

    @model_validator(mode="after")
    def _normalise_dt(self) -> LatestReadingContract:
        self.datetime = _ensure_utc(self.datetime)
        return self


class MeasureContract(_BaseContract):
    id: str = Field(alias="@id")
    label: str | None = None
    unit: str | None = None
    unit_name: str | None = Field(default=None, alias="unitName")
    datum_type: str | None = Field(default=None, alias="datumType")
    qualifier: str | None = None
    period: int | None = None
    notation: str | None = None
    latest_reading_value: float | None = None
    latest_reading_datetime: _dt.datetime | None = None
    latest_reading_is_list: bool = False
    latest_reading_missing: bool = False

    @model_validator(mode="before")
    @classmethod
    def _flatten(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        out = dict(data)
        latest = out.pop("latestReading", None)
        if isinstance(latest, dict):
            raw = latest.get("value")
            if raw is None:
                out["latest_reading_missing"] = True
            elif isinstance(raw, list):
                out["latest_reading_value"] = float(raw[0]) if raw else None
                out["latest_reading_is_list"] = True
            else:
                out["latest_reading_value"] = float(raw)
            dt = latest.get("dateTime")
            if dt is not None:
                out["latest_reading_datetime"] = dt
        else:
            out["latest_reading_missing"] = True
        return out

    @model_validator(mode="after")
    def _normalise(self) -> MeasureContract:
        self.latest_reading_datetime = _ensure_utc(self.latest_reading_datetime)
        return self


class StationContract(_BaseContract):
    id: str = Field(alias="@id")
    station_reference: str | None = Field(default=None, alias="stationReference")
    label: str | None = None
    town: str | None = None
    river_name: str | None = Field(default=None, alias="riverName")
    catchment_name: str | None = Field(default=None, alias="catchmentName")
    rloi_id: int | None = Field(default=None, alias="RLOIid")
    status: str | None = None
    lat: float | None = None
    long: float | None = None
    easting: float | None = None
    northing: float | None = None
    date_opened: str | None = Field(default=None, alias="dateOpened")
    measures: list[str] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _flatten(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        out = dict(data)
        # Quirk 3: lat/easting observed as single-element lists
        for key in ("lat", "long", "easting", "northing"):
            if key in out:
                out[key] = _coerce_numeric(out[key])
        # RLOIid arrives as a string in some payloads
        if out.get("RLOIid") is not None:
            try:
                out["RLOIid"] = int(out["RLOIid"])
            except (TypeError, ValueError):
                out["RLOIid"] = None
        # measures[] holds @id URIs; keep as plain strings
        raw_measures = out.get("measures")
        if isinstance(raw_measures, list):
            ids = []
            for m in raw_measures:
                if isinstance(m, dict):
                    ids.append(str(m.get("@id", "")))
                else:
                    ids.append(str(m))
            out["measures"] = [m for m in ids if m]
        elif raw_measures is None:
            out["measures"] = []
        return out


class FloodAreaContract(_BaseContract):
    """Nested ``floodArea`` inside a flood warning — flattened by the parent."""

    id: str | None = Field(default=None, alias="@id")
    label: str | None = None
    description: str | None = None


class FloodWarningContract(_BaseContract):
    id: str = Field(alias="@id")
    severity: str | None = None
    severity_level: int | None = Field(default=None, alias="severityLevel")
    situation: str | None = None
    time_raised: _dt.datetime | None = Field(default=None, alias="timeRaised")
    flood_area_id: str | None = None
    flood_area_label: str | None = None
    flood_area_description: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _flatten(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        out = dict(data)
        area = out.pop("floodArea", None)
        if isinstance(area, dict):
            out["flood_area_id"] = area.get("@id")
            out["flood_area_label"] = area.get("label")
            out["flood_area_description"] = area.get("description")
        return out

    @model_validator(mode="after")
    def _normalise_dt(self) -> FloodWarningContract:
        # Quirk 6: flood timestamps have no timezone — assume UTC
        self.time_raised = _ensure_utc(self.time_raised)
        return self


class ReadingContract(_BaseContract):
    id: str = Field(alias="@id")
    measure: str
    datetime: _dt.datetime = Field(alias="dateTime")
    value_raw: float | None = None
    value_is_list: bool = False
    value_missing: bool = False
    qualifier: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _flatten(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        out = dict(data)
        raw = out.pop("value", None)
        if raw is None:
            out["value_missing"] = True
        elif isinstance(raw, list):
            out["value_raw"] = float(raw[0]) if raw else None
            out["value_is_list"] = True
        else:
            out["value_raw"] = float(raw)
        return out

    @model_validator(mode="after")
    def _normalise_dt(self) -> ReadingContract:
        self.datetime = _ensure_utc(self.datetime)
        return self
