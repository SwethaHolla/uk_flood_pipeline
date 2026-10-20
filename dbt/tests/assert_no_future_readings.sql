-- Readings timestamps must never be in the future (a 1-day grace covers
-- clock skew between the EA API and the warehouse).
select
    reading_id,
    measure_id,
    reading_time
from {{ ref('stg_readings') }}
where reading_time > {{ dbt.current_timestamp() }} + interval 1 day
