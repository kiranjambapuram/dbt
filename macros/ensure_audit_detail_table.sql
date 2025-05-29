{#
This macro creates the `dbt_audit_detail` table in the target schema
(i.e., `{{ target.schema }}.dbt_audit_detail`) if it doesn't already exist.
It uses a `CREATE TABLE IF NOT EXISTS` statement. This table is intended
for storing more granular, step-by-step logging details during model runs.

The defined schema includes:
- detail_log_id VARCHAR(255) PRIMARY KEY: Unique ID for the detail log entry.
- invocation_id VARCHAR(255): Links to dbt_audit_summary.
- model_name VARCHAR(255): Name of the model being processed.
- step_name VARCHAR(255): Name of the specific step or event being logged.
- step_sequence_num INTEGER NULL: Optional sequence number for ordered steps.
- event_timestamp TIMESTAMP: Timestamp of the specific event.
- status VARCHAR(50): Status of the event (e.g., 'info', 'debug', 'warning', 'error').
- message VARCHAR(1000): Detailed message for the log entry.

Optimization:
This macro is optimized to run its DDL (`CREATE TABLE IF NOT EXISTS`) only
once per `dbt run`. It uses a global flag (`flags.AUDIT_DETAIL_TABLE_ENSURED`)
to track if it has already successfully executed in the current run.
Subsequent calls within the same run will be skipped.

Primary Usage:
It is primarily intended to be called by the `log_step_event` macro
to ensure the detail audit table is available before logging granular events.
#}
{% macro create_dbt_audit_detail_table() %}
    {% if flags.get('AUDIT_DETAIL_TABLE_ENSURED', False) %}
        {% do log("Audit detail table 'dbt_audit_detail' already ensured in this run. Skipping DDL.", info=True) %}
        {% return '' %}
    {% endif %}

{% set create_detail_table_sql %}
CREATE TABLE IF NOT EXISTS {{ target.schema }}.dbt_audit_detail (
    detail_log_id VARCHAR(255) PRIMARY KEY,
    invocation_id VARCHAR(255),
    model_name VARCHAR(255),
    step_name VARCHAR(255),
    step_sequence_num INTEGER NULL,
    event_timestamp TIMESTAMP,
    status VARCHAR(50),
    message VARCHAR(1000)
);
{% endset %}

{{ log("Attempting to ensure audit detail table 'dbt_audit_detail' exists (DDL execution or check)...", info=True) }}
{% do run_query(create_detail_table_sql) %}
{% do flags.set('AUDIT_DETAIL_TABLE_ENSURED', True) %}
{% do log("Audit detail table 'dbt_audit_detail' ensured. Flag 'AUDIT_DETAIL_TABLE_ENSURED' set to True.", info=True) %}
{% endmacro %}
