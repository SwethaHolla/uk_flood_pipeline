{#-
  Unified reading shape. The contract already normalised timestamps to UTC and
  split the quirky `value` into value_raw + value_is_list / value_missing
  (PRD §7 quirks 1 & 2); staging just renames for clarity.
-#}
with source as (
    select * from {{ source('raw', 'readings') }}
)

select
    id as reading_id,
    measure as measure_id,
    datetime as reading_time,
    cast(datetime as date) as reading_date,
    value_raw as reading_value,
    value_is_list,
    value_missing,
    {{ clean_categorical('qualifier') }} as qualifier,
    _dlt_load_id,
    _dlt_id
from source
