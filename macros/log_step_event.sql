{#
This macro logs a detailed event or step within a model's execution
into the `dbt_audit_detail` table. This allows for granular tracking
of multi-step processes within a single model run.

It ensures the `dbt_audit_detail` table exists by calling
`create_dbt_audit_detail_table()` before attempting to log.

Arguments:
- step_name (string): A descriptive name for the step or event being logged.
- message (string, optional): A detailed message for this event.
                              Defaults to NULL if not provided or empty.
- status (string, optional): The status of this event (e.g., 'info', 'debug',
                             'warning', 'error'). Defaults to 'info'.
- model_name_arg (string, optional): The name of the model or process.
                                   If None, `this.name` (current model name) is used.
- step_sequence_num (integer, optional): An optional sequence number for ordering steps.
                                         Defaults to NULL if not provided.
#}
{% macro log_step_event(step_name, message=None, status='info', model_name_arg=None, step_sequence_num=None) %}
    {# Ensure the audit detail table exists #}
    {% do create_dbt_audit_detail_table() %}

    {# Determine the model name to log against #}
    {% set model_name_to_log = model_name_arg if model_name_arg is not none else this.name %}

    {# Generate a practically unique detail_log_id #}
    {# Combines timestamp, step name, and a random number for uniqueness #}
    {% set detail_log_id_str = dbt.current_timestamp_in_utc().isoformat() ~ "_" ~ step_name|replace(" ", "_") ~ "_" ~ range(1000, 9999) | random %}
    {% set detail_log_id_value = "'" ~ detail_log_id_str ~ "'" %}

    {# Prepare other values for SQL insertion #}
    {% set invocation_id_value = "'" ~ invocation_id ~ "'" %}
    {% set model_name_value = "'" ~ model_name_to_log ~ "'" %}
    {% set step_name_value = "'" ~ step_name ~ "'" %}
    {% set event_timestamp_sql = dbt.current_timestamp() %} {# Use dbt's cross-db current timestamp function #}
    {% set status_value = "'" ~ status ~ "'" %}
    {% set message_value = "'" ~ message ~ "'" if message and message|trim != "" else "NULL" %}
    {% set step_sequence_num_value = step_sequence_num if step_sequence_num is not none else "NULL" %}

    {# Construct the INSERT statement #}
    {% set insert_sql %}
    INSERT INTO {{ target.schema }}.dbt_audit_detail (
        detail_log_id,
        invocation_id,
        model_name,
        step_name,
        step_sequence_num,
        event_timestamp,
        status,
        message
    ) VALUES (
        {{ detail_log_id_value }},
        {{ invocation_id_value }},
        {{ model_name_value }},
        {{ step_name_value }},
        {{ step_sequence_num_value }},
        {{ event_timestamp_sql }},
        {{ status_value }},
        {{ message_value }}
    );
    {% endset %}

    {# Execute the INSERT statement #}
    {% do run_query(insert_sql) %}

    {# Optional: Log to dbt console for immediate feedback during development/debugging #}
    {# {% do log("Step Event: [" ~ model_name_to_log ~ "] " ~ step_name ~ " (" ~ status ~ ") - " ~ message, info=(status=='info')) %} #}

{% endmacro %}
