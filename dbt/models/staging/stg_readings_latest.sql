with source as (
    select * from {{ source('raw', 'readings_latest') }}
)

select
    id as reading_id,
    measure as measure_id,
    datetime as reading_time,
    value_raw as reading_value,
    value_is_list,
    value_missing,
    {{ clean_categorical('qualifier') }} as qualifier,
    _dlt_load_id
from source
