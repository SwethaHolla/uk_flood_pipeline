select distinct
    flood_area_id,
    flood_area_label,
    flood_area_description
from {{ ref('stg_flood_warnings') }}
where flood_area_id is not null
