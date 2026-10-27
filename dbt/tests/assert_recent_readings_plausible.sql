{{ config(severity='warn', tags=['recent']) }}

select
    reading_id,
    measure_id,
    reading_time,
    reading_value
from {{ ref('fct_readings') }}
where reading_date >= {{ test_lookback_date_start() }}
  and is_plausible is false
