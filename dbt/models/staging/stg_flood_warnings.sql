with source as (
    select * from {{ source('raw', 'flood_warnings') }}
)

select
    id as warning_id,
    {{ clean_categorical('severity') }} as severity,
    severity_level,
    situation,
    -- Quirk 6: the contract promoted naive timestamps to UTC already
    time_raised,
    flood_area_id,
    flood_area_label,
    flood_area_description,
    _dlt_load_id
from source
