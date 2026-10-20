{#-
  Incremental fact: partitioned by reading_date and clustered by measure_id on
  BigQuery (adapter-gated below); DuckDB (local dev) runs plain incremental
  MERGE. Benchmark evidence: docs/benchmarks.md.
-#}
{% if target.type == 'bigquery' %}
    {{
        config(
            materialized='incremental',
            incremental_strategy='merge',
            unique_key=['measure_id', 'reading_time'],
            partition_by={'field': 'reading_date', 'data_type': 'date'},
            cluster_by=['measure_id']
        )
    }}
{% else %}
    {{
        config(
            materialized='incremental',
            incremental_strategy='delete+insert',
            unique_key=['measure_id', 'reading_time']
        )
    }}
{% endif %}

select
    r.reading_id,
    r.measure_id,
    m.station_id,
    r.reading_time,
    r.reading_date,
    r.reading_value,
    r.value_is_list,
    r.value_missing,
    r.qualifier
from {{ ref('stg_readings') }} r
left join {{ ref('dim_measure') }} m on r.measure_id = m.measure_id
{% if is_incremental() %}
where r.reading_time > (
    select coalesce(max(reading_time), cast('1900-01-01' as timestamp))
    from {{ this }}
)
{% endif %}
