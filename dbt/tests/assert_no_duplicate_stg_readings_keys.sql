-- Staging owns raw dedup: keep the latest _dlt_load_id per (measure, datetime).
-- Duplicates here mean the staging QUALIFY is broken; fct_readings also checks
-- the same natural key after its incremental partition rebuild.
select
    measure_id,
    reading_time,
    count(*) as row_count
from {{ ref('stg_readings') }}
where reading_time >= {{ test_lookback_timestamp_start() }}
group by 1, 2
having count(*) > 1
