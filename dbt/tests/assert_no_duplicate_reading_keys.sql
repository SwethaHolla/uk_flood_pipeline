-- PRD success criterion 2 (warehouse-side half): the natural key of a reading
-- is (measure_id, reading_time). Duplicates here mean the incremental merge
-- is broken.
select
    measure_id,
    reading_time,
    count(*) as row_count
from {{ ref('fct_readings') }}
where reading_date >= {{ test_lookback_date_start() }}
group by 1, 2
having count(*) > 1
