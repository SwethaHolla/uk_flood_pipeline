with source as (
    select * from {{ source('raw', 'stations') }}
)

select
    id as station_id,
    station_reference,
    label,
    town,
    river_name,
    catchment_name,
    rloi_id,
    {{ clean_categorical('status') }} as status,
    lat,
    long,
    easting,
    northing,
    try_cast(date_opened as date) as date_opened,
    _dlt_load_id,
    _dlt_id
from source
