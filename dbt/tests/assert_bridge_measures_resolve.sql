-- Every measure URI referenced by some station must resolve to dim_measure.
-- Orphans mean the bridge and the measures ingestion have drifted apart.
select
    b.measure_uri,
    count(*) as station_count
from {{ ref('stg_station_measures') }} b
left join {{ ref('dim_measure') }} m on b.measure_uri = m.measure_id
where m.measure_id is null
group by 1
