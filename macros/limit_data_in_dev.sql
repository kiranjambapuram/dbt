{% macro limit_data_in_dev(column_name, dev_days_of_data=3) %}
{% if target.database == 'dbt_kjambapuram' %}
    where {{ column_name }} >+ dateadd('day', - {{ dev_days_of_data }}, current_timestamp)
{% endif %}
{%- endmacro %}