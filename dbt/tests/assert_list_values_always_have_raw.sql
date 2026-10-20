-- PRD §7 quirk 1: when the API returned a list value (e.g. [0.0, 0.0]) the
-- contract must have captured the head of the list in reading_value. A flag
-- without a value means data was silently dropped.
select
    reading_id,
    measure_id,
    reading_time
from {{ ref('stg_readings') }}
where value_is_list and reading_value is null
