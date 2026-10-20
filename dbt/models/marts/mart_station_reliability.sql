with window_readings as (
    select
        f.station_id,
        f.measure_id,
        f.reading_time,
        f.value_missing
    from {{ ref('fct_readings') }} f
    where f.reading_time >= {{ dbt.current_timestamp() }} - interval {{ var('reliability_window_days') }} day
),

by_station as (
    select
        station_id,
        count(*) as readings_7d,
        count(distinct measure_id) as measures_reporting,
        sum(case when value_missing then 1 else 0 end) * 100.0
            / nullif(count(*), 0) as missing_value_pct,
        max(reading_time) as last_reading_at
    from window_readings
    group by station_id
)

select
    s.station_id,
    s.label as station_label,
    s.town,
    s.river_name,
    coalesce(r.readings_7d, 0) as readings_7d,
    coalesce(r.readings_7d, 0) / {{ var('reliability_window_days') }}.0 as readings_per_day,
    coalesce(r.measures_reporting, 0) as measures_reporting,
    round(coalesce(r.missing_value_pct, 0), 2) as missing_value_pct,
    r.last_reading_at,
    case
        when r.last_reading_at is null then 'no_data'
        when r.last_reading_at >= {{ dbt.current_timestamp() }} - interval 1 day then 'fresh'
        when r.last_reading_at >= {{ dbt.current_timestamp() }} - interval 3 day then 'stale'
        else 'offline'
    end as freshness_status
from {{ ref('dim_station') }} s
left join by_station r on s.station_id = r.station_id
