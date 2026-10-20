with rainfall_readings as (
    select
        f.station_id,
        f.reading_time,
        f.reading_value
    from {{ ref('fct_readings') }} f
    join {{ ref('dim_measure') }} m on f.measure_id = m.measure_id
    where
        lower(m.label) like '%rainfall%'
        and f.reading_time >= {{ dbt.current_timestamp() }} - interval {{ var('trend_window_days') }} day
)

select
    r.station_id,
    s.label as station_label,
    s.town,
    {{ dbt.date_trunc('hour', 'r.reading_time') }} as hour_bucket,
    count(*) as readings,
    round(coalesce(sum(r.reading_value), 0), 2) as rainfall_total,
    round(max(r.reading_value), 2) as rainfall_peak
from rainfall_readings r
left join {{ ref('dim_station') }} s on r.station_id = s.station_id
group by 1, 2, 3, 4
