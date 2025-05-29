{#
This macro logs the start of a model run or a specific process step
into the `dbt_audit_summary` table.

It ensures the `dbt_audit_summary` table exists by calling
`create_dbt_audit_summary_table()` before attempting to log.

Arguments:
- model_name_arg (string, optional): The name of the model or process.
                                   If None, `this.name` (current model name) is used.
- message (string, optional): A custom message to include with this start event.
                              Defaults to NULL if not provided or if empty.
#}
{% macro log_run_start(model_name_arg=None, message=None) %}
    {# Ensure the audit summary table exists #}
    {% do create_dbt_audit_summary_table() %}

    {# Determine the model name to log #}
    {% set model_name_to_log = model_name_arg if model_name_arg is not none else this.name %}

    {# Prepare values for SQL insertion #}
    {% set invocation_id_value = "'" ~ invocation_id ~ "'" %}
    {% set model_name_value = "'" ~ model_name_to_log ~ "'" %}
    {% set run_by_user_value = "'" ~ target.user ~ "'" %}
    {% set custom_message_value = "'" ~ message ~ "'" if message and message|trim != "" else "NULL" %}
    {% set run_started_at_sql_value = dbt.current_timestamp() %} {# Use dbt's cross-db current timestamp function #}

    {# Construct the INSERT statement #}
    {% set insert_sql %}
    INSERT INTO {{ target.schema }}.dbt_audit_summary (
        invocation_id,
        model_name,
        run_started_at,
        run_by_user,
        status,
        custom_message
        -- run_ended_at, execution_time_seconds, rows_processed are intentionally omitted
        -- as they will be set by a corresponding 'log_run_end' or similar macro.
    ) VALUES (
        {{ invocation_id_value }},
        {{ model_name_value }},
        {{ run_started_at_sql_value }},
        {{ run_by_user_value }},
        'started',
        {{ custom_message_value }}
    );
    {% endset %}

    {# Execute the INSERT statement #}
    {% do run_query(insert_sql) %}

    {% do log("Run start logged for '" ~ model_name_to_log ~ "'. Invocation ID: " ~ invocation_id, info=True) %}

{% endmacro %}
