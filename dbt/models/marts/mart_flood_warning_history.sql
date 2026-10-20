select
    f.warning_id,
    f.flood_area_id,
    f.flood_area_label,
    f.severity,
    f.severity_level,
    f.situation,
    f.time_raised,
    f.hours_since_raised,
    case f.severity_level
        when 4 then 'Severe Flood Warning'
        when 3 then 'Flood Warning'
        when 2 then 'Flood Alert'
        when 1 then 'Flood Watch'
        else 'Unknown'
    end as severity_label
from {{ ref('fct_flood_warnings') }} f
