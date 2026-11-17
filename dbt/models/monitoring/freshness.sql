{{ config(materialized='view', tags=['monitoring']) }}

with readings_latest as (
    select max(datetime) as max_reading_time
    from {{ source('raw', 'readings_latest') }}
),

readings as (
    select max(datetime) as max_reading_time
    from {{ source('raw', 'readings') }}
),

fct as (
    select max(reading_time) as max_reading_time
    from {{ ref('fct_readings') }}
)
{% if target.type == 'bigquery' %}
, gates as (
    select
        countif(decision = false) as gate_trips_24h,
        max(case when decision = false then event_time end) as last_gate_trip_at,
        max(case when decision = false then reason end) as last_gate_trip_reason,
        countif(decision = false and alert_sent) as gate_alerts_24h
    from {{ source('ops', 'gate_events') }}
    where event_time >= current_timestamp - interval 24 hour
)
{% endif %}

select
    readings_latest.max_reading_time as readings_latest_max,
    {% if target.type == 'bigquery' %}
    timestamp_diff(current_timestamp, readings_latest.max_reading_time, minute)
    {% else %}
    date_diff('minute', readings_latest.max_reading_time, current_timestamp)
    {% endif %} as readings_latest_lag_minutes,
    readings.max_reading_time as readings_max,
    {% if target.type == 'bigquery' %}
    timestamp_diff(current_timestamp, readings.max_reading_time, minute)
    {% else %}
    date_diff('minute', readings.max_reading_time, current_timestamp)
    {% endif %} as readings_lag_minutes,
    fct.max_reading_time as fct_readings_max,
    {% if target.type == 'bigquery' %}
    timestamp_diff(current_timestamp, fct.max_reading_time, minute)
    {% else %}
    date_diff('minute', fct.max_reading_time, current_timestamp)
    {% endif %} as fct_readings_lag_minutes,
    {% if target.type == 'bigquery' %}
    gates.gate_trips_24h,
    gates.last_gate_trip_at,
    gates.last_gate_trip_reason,
    gates.gate_alerts_24h
    {% else %}
    cast(null as bigint) as gate_trips_24h,
    cast(null as timestamp) as last_gate_trip_at,
    cast(null as varchar) as last_gate_trip_reason,
    cast(null as bigint) as gate_alerts_24h
    {% endif %}
from readings_latest
cross join readings
cross join fct
{% if target.type == 'bigquery' %}
cross join gates
{% endif %}
