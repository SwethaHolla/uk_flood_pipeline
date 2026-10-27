# Data Dictionary — UK Flood Pipeline

Scope: the canonical curated models (`stg_*` → `dim_*`/`fct_*`). Raw tables mirror
the EA API payloads and are not re-documented here.

**Generated from a live query** against `ukfloodpipeline`. Values are observed
Unit values; they are **not** a fixed contract and will drift as the EA API changes.

---

## `fct_readings`

| Column | Type | Description |
|---|---|---|
| `reading_id` | STRING | EA natural key (`@id`). Unique. |
| `measure_id` | STRING | FK → `dim_measure.measure_id`. |
| `reading_time` | TIMESTAMP (UTC) | Reading datetime. Partition key (`reading_date`). |
| `reading_date` | DATE | `DATE(reading_time)`; BigQuery partition column (DAY). |
| `reading_value` | FLOAT64 | NULL when the API omitted the value (`value_missing`). |
| `qualifier` | STRING | EA qualifier (e.g. `Unchecked`, `Downstream`). |

## `stg_measures.unit_name` — known API quirk (documented Phase B)

The EA API returns **more unit strings than its own documentation lists**. The
`accepted_values` test on `stg_measures.unit_name` is deliberately configured
`severity: warn` (see `dbt/models/staging/schema.yml`). Observed off-list raw
values (live count, 2026-10-22):

| Raw `unit_name` | Rows | Note |
|---|---|---|
| `mASD` | 1859 | metres above station datum |
| `mAOD` | 1463 | metres above ordnance datum |
| `m3/s` | 457 | whitelist uses superscript `m³/s`; ASCII form is unlisted |
| `mBDAT` | 116 | metres below datum |
| `Ml/d` | 33 | megalitres/day |
| `deg C` | 26 | degrees Celsius (ASCII) |
| `V` | 21 | volts |
| `m/s` | 15 | metres/second |
| `deg` | 10 | degrees |
| `%` | 4 | percent |
| `Knots` | 3 | wind/flow knots (capital K) — cf. whitelist `knots` |
| `Knot` | 2 | singular |
| `m2` | 1 | square metres |
| `mV` | 1 | millivolts |
| `mACD` | 1 | metres above chart datum |

Also present but normalised/whitelisted: `m`, `mm`, `m3/s` (mapped), plus the
`---` placeholder which the cleaner maps to NULL.

**Decision:** keep `severity: warn`. Extend the whitelist only when a unit is
confirmed real and stable; do not silently accept arbitrary strings. A spike in
off-list distinct values should be visible in the observability layer (Phase C).

## `dim_station`

| Column | Type | Description |
|---|---|---|
| `station_id` | STRING | EA natural key. Unique. |
| `station_reference` | STRING | EA station reference. |
| `label` | STRING | Human-readable name. |
| `town` / `river_name` / `catchment_name` | STRING | Location descriptors. |
| `lat` / `long` | FLOAT64 | WGS84. Lists coerced to first element (quirk 3). |
| `easting` / `northing` | FLOAT64 | OSGB grid. |
| `status` | STRING | Station status. |

## `fct_flood_warnings`

| Column | Type | Description |
|---|---|---|
| `warning_id` | STRING | EA natural key. Unique. |
| `severity_level` | INT64 | 1–4 (4 = severe). |
| `time_raised` | TIMESTAMP (UTC) | Naive API timestamps promoted to UTC (quirk 6). |
| `flood_area_id` / `flood_area_label` | STRING | Flattened `floodArea` (quirk 5). |
