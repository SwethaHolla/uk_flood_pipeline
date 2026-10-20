with measures as (
    select * from {{ ref('stg_measures') }}
),

station_bridge as (
    select
        measure_uri,
        any_value(station_id) as station_id
    from {{ ref('stg_station_measures') }}
    group by measure_uri
)

select
    m.measure_id,
    b.station_id,  -- measures don't carry a station field; the bridge derives it
    m.label,
    m.unit,
    m.unit_name,
    m.datum_type,
    m.qualifier,
    m.period,
    m.notation,
    m.latest_reading_value,
    m.latest_reading_datetime,
    m.latest_reading_is_list,
    m.latest_reading_missing
from measures m
left join station_bridge b on m.measure_id = b.measure_uri
