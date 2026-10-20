select
    w.warning_id,
    w.flood_area_id,
    a.flood_area_label,
    a.flood_area_description,
    w.severity,
    w.severity_level,
    w.situation,
    w.time_raised,
    {{ dbt.datediff('w.time_raised', dbt.current_timestamp(), 'hour') }} as hours_since_raised
from {{ ref('stg_flood_warnings') }} w
left join {{ ref('dim_flood_area') }} a on w.flood_area_id = a.flood_area_id
