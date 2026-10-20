{#-
  Bridge between stations and measures. stations.measures is an array of
  measure @ids; dlt normalises it into the stations__measures child table
  (one row per item, keyed by _dlt_parent_id).
-#}
select
    s.station_id,
    m.value as measure_uri
from {{ source('raw', 'stations__measures') }} m
join {{ ref('stg_stations') }} s on m._dlt_parent_id = s._dlt_id
where m.value is not null
