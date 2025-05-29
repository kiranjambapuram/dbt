{#
This macro updates an existing record in the `dbt_audit_summary` table
to mark the end of a model run or a specific process step. It sets the
run_ended_at timestamp, calculates the execution_time_seconds, and updates
the status.

It expects a corresponding record to have already been inserted by
`log_run_start` (or a similar macro) with the same invocation_id and model_name.

Arguments:
- model_name_arg (string, optional): The name of the model or process.
                                   If None, `this.name` (current model name) is used.
- status (string, optional): The final status of the run. Defaults to 'completed'.
                             Other common values might be 'failed', 'skipped', etc.
#}
{% macro log_run_end(model_name_arg=None, status='completed') %}
    {# Determine the model name to log against #}
    {% set model_name_to_log = model_name_arg if model_name_arg is not none else this.name %}

    {# Prepare values for SQL update #}
    {% set run_ended_at_sql_value = dbt.current_timestamp() %} {# Use dbt's cross-db current timestamp function #}
    {% set invocation_id_value = "'" ~ invocation_id ~ "'" %}
    {% set model_name_value = "'" ~ model_name_to_log ~ "'" %}
    {% set status_value = "'" ~ status ~ "'" %}

    {# Calculate execution time.
       This assumes run_started_at is a column in the dbt_audit_summary table.
       dbt.datediff will generate the appropriate SQL for the target database. #}
    {% set execution_time_seconds_sql %}
        {{ dbt.datediff('run_started_at', run_ended_at_sql_value, 'second') }}
    {% endset %}

    {# Construct the UPDATE statement #}
    {% set update_sql %}
    UPDATE {{ target.schema }}.dbt_audit_summary
    SET
        run_ended_at = {{ run_ended_at_sql_value }},
        status = {{ status_value }},
        execution_time_seconds = ( {{ execution_time_seconds_sql }} )
    WHERE
        invocation_id = {{ invocation_id_value }}
        AND model_name = {{ model_name_value }}
        AND run_ended_at IS NULL {# Good practice: only update if not already ended #}
    ;
    {% endset %}

    {# Execute the UPDATE statement #}
    {% set results = run_query(update_sql) %} {# Store results to check rows affected if needed, though not strictly necessary here #}

    {% do log("Run end logged for '" ~ model_name_to_log ~ "'. Status: " ~ status ~ ". Invocation ID: " ~ invocation_id, info=True) %}

{% endmacro %}
