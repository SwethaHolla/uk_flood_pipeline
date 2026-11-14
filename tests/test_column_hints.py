"""Regression tests for explicit dlt column hints (Phase B schema-drift fix).

dlt infers column types from each batch and silently omits columns that are
all-NULL in that batch. A live reload into a fresh dataset dropped `qualifier`
(readings) and `situation` (flood_warnings), which broke the staging models with
`Unrecognized name`. Every contract field must therefore carry an explicit type
hint so the physical schema is stable regardless of the batch contents.
"""

from __future__ import annotations

from uk_flood.pipelines import RESOURCE_SPECS, TABLE_COLUMNS

# Nested list fields are unnested by dlt into child tables, so they are not
# declared on the parent resource.
_DLT_NESTED_FIELDS = {"stations": {"measures"}}


def test_every_contract_field_has_a_column_hint():
    for name, spec in RESOURCE_SPECS.items():
        contract_fields = set(spec["contract"].model_fields)
        declared = set(TABLE_COLUMNS[spec["table"]])
        missing = contract_fields - declared - _DLT_NESTED_FIELDS.get(name, set())
        assert not missing, f"{spec['table']} is missing column hints: {sorted(missing)}"


def test_readings_history_reuses_latest_hints():
    assert TABLE_COLUMNS["readings"] == TABLE_COLUMNS["readings_latest"]


def test_all_null_prone_columns_are_declared():
    assert "qualifier" in TABLE_COLUMNS["readings"]
    assert "qualifier" in TABLE_COLUMNS["readings_latest"]
    assert "situation" in TABLE_COLUMNS["flood_warnings"]
