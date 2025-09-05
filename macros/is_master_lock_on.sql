{% macro is_master_lock_on(period) %}

    {% set lock_check_query %}
    select 1
    from {{ ref('dim_lock') }}
    where period = '{{ period }}'
      and type = 'Master'
      and lock_status = TRUE
    limit 1
    {% endset %}

    {% set results = run_query(lock_check_query) %}

    {% if results %}
        {{ return('1') }}
    {% else %}
        {{ return('0') }}
    {% endif %}

{% endmacro %}
