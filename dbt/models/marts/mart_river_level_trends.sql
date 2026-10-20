with level_readings as (
    select
        f.station_id,
        f.measure_id,
        f.reading_time,
        f.reading_value
    from {{ ref('fct_readings') }} f
    join {{ ref('dim_measure') }} m on f.measure_id = m.measure_id
    where
        lower(m.label) like '%level%'
        and f.reading_time >= {{ dbt.current_timestamp() }} - interval {{ var('trend_window_days') }} day
        and f.reading_value is not null
)

select
    l.station_id,
    m.station_id is not null as station_known,
    m.label as station_label,
    {{ dbt.date_trunc('hour', 'l.reading_time') }} as hour_bucket,
    count(*) as readings,
    round(avg(l.reading_value), 3) as avg_level,
    round(min(l.reading_value), 3) as min_level,
    round(max(l.reading_value), 3) as max_level
from level_readings l
left join {{ ref('dim_station') }} m on l.station_id = m.station_id
group by 1, 2, 3, 4
