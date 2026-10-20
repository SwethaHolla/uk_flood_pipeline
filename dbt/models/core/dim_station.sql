select
    station_id,
    station_reference,
    label,
    town,
    river_name,
    catchment_name,
    rloi_id,
    status,
    lat,
    long,
    easting,
    northing,
    date_opened
from {{ ref('stg_stations') }}
