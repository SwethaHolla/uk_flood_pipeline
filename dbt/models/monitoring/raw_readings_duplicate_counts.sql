{{ config(materialized='view', tags=['monitoring']) }}

with grouped as (
    select
        measure,
        datetime,
        count(*) as row_count
    from {{ source('raw', 'readings') }}
    where datetime >= {{ test_lookback_timestamp_start() }}
    group by 1, 2
)

select
    cast(datetime as date) as reading_date,
    sum(row_count) as raw_rows,
    count(*) as distinct_keys,
    sum(row_count - 1) as duplicate_extra_rows
from grouped
group by 1
