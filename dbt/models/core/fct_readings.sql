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
            partition_expiration_days=90,
            cluster_by=['measure_id'],
            on_schema_change='append_new_columns'
        )
    }}
{% else %}
    {{
        config(
            materialized='incremental',
            incremental_strategy='delete+insert',
            unique_key=['measure_id', 'reading_time'],
            on_schema_change='append_new_columns'
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
    r.qualifier,
    case
        when r.reading_value is null then true
        when m.unit_name = 'mm' and lower(r.measure_id) like '%rainfall%'
            then r.reading_value between -1 and 1000
        when m.unit_name = 'mm' and lower(r.measure_id) like '%level%'
            then r.reading_value between -10000 and 30000
        when m.unit_name in ('mASD', 'm', 'mAOD', 'mBDAT')
            then r.reading_value between -100 and 1000
        when m.unit_name = 'm3/s' then r.reading_value between -10000 and 10000
        when m.unit_name = 'Ml/d' then r.reading_value between -1000000 and 1000000
        when m.unit_name = 'deg C' then r.reading_value between -30 and 60
        when m.unit_name = 'deg' then r.reading_value between -360 and 360
        when m.unit_name = 'm/s' then r.reading_value between 0 and 100
        when m.unit_name = '%' then r.reading_value between 0 and 100
        when m.unit_name = 'Knots' then r.reading_value between -1 and 200
        when m.unit_name = 'V' then r.reading_value between 0 and 100
        else true
    end as is_plausible
from {{ ref('stg_readings') }} r
left join {{ ref('dim_measure') }} m on r.measure_id = m.measure_id
{% if is_incremental() %}
where r.reading_time > (
    select coalesce(max(reading_time), cast('1900-01-01' as timestamp))
    from {{ this }}
)
{% endif %}
