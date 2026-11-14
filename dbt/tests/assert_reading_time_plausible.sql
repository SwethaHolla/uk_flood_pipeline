-- Reading timestamps must sit inside plausible bounds: not before the EA
-- archive begins (2000-01-01) and not more than a day in the future (clock
-- skew grace). Ingest also enforces these bounds (contracts.py).
select
    reading_id,
    measure_id,
    reading_time
from {{ ref('stg_readings') }}
where reading_time >= {{ test_lookback_timestamp_start() }}
  and (
      reading_time < timestamp '2000-01-01'
      or reading_time > {{ dbt.current_timestamp() }} + interval 1 day
  )
