{% macro test_lookback_timestamp_start() %}
    current_timestamp - interval {{ var('test_lookback_days', 3) }} day
{% endmacro %}

{% macro test_lookback_date_start() %}
    current_date - interval {{ var('test_lookback_days', 3) }} day
{% endmacro %}
