{% macro clean_categorical(column_name) %}
    {#-
      Dirty categoricals observed in the live API (PRD §7 quirk 8):
      '', '---', 'None' all mean "no value". Raw values stay in the raw layer.
    -#}
    case
        when trim({{ column_name }}) in ('', '---', 'None') then null
        else trim({{ column_name }})
    end
{% endmacro %}
