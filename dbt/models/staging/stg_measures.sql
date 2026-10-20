with source as (
    select * from {{ source('raw', 'measures') }}
)

select
    id as measure_id,
    label,
    unit,
    {{ clean_categorical('unit_name') }} as unit_name,
    {{ clean_categorical('datum_type') }} as datum_type,
    {{ clean_categorical('qualifier') }} as qualifier,
    period,
    notation,
    latest_reading_value,
    latest_reading_datetime,
    latest_reading_is_list,
    latest_reading_missing,
    _dlt_load_id,
    _dlt_id
from source
