{#
This macro creates the `dbt_audit_summary` table in the target schema
(i.e., `{{ target.schema }}.dbt_audit_summary`) if it doesn't already exist.
It uses a `CREATE TABLE IF NOT EXISTS` statement. The defined schema includes
columns for invocation details, model name, timestamps, status, user,
and a `custom_message` field.

Optimization:
This macro is optimized to run its DDL (`CREATE TABLE IF NOT EXISTS`) only
once per `dbt run`. It uses a global flag (`flags.AUDIT_SUMMARY_TABLE_ENSURED`)
to track if it has already successfully executed in the current run.
Subsequent calls within the same run will be skipped, improving performance
when this macro is called multiple times (e.g., by `log_model_run`).

Primary Usage:
It is primarily called by the `log_run_start` macro to ensure the audit
summary table is available before logging.

Standalone Usage:
It can also be used standalone, for example, in an `on-run-start` hook in
`dbt_project.yml` if the table is needed by other processes or for explicit
setup at the beginning of a run:

on-run-start:
  - "{{ create_dbt_audit_summary_table() }}"
#}
{% macro create_dbt_audit_summary_table() %}
    {% if flags.get('AUDIT_SUMMARY_TABLE_ENSURED', False) %}
        {% do log("Audit summary table 'dbt_audit_summary' already ensured in this run. Skipping DDL.", info=True) %}
        {% return '' %}
    {% endif %}

{% set create_table_sql %}
CREATE TABLE IF NOT EXISTS {{ target.schema }}.dbt_audit_summary (
    invocation_id VARCHAR(255),
    model_name VARCHAR(255),
    run_started_at TIMESTAMP,
    run_ended_at TIMESTAMP,
    execution_time_seconds INTEGER,
    rows_processed INTEGER,
    status VARCHAR(50),
    run_by_user VARCHAR(255),
    custom_message VARCHAR(1000) NULL,
    PRIMARY KEY (invocation_id, model_name)
);
{% endset %}
{{ log("Attempting to ensure audit summary table 'dbt_audit_summary' exists (DDL execution or check)...", info=True) }}
{% do run_query(create_table_sql) %}
{% do flags.set('AUDIT_SUMMARY_TABLE_ENSURED', True) %}
{% do log("Audit summary table 'dbt_audit_summary' ensured. Flag 'AUDIT_SUMMARY_TABLE_ENSURED' set to True.", info=True) %}
{% endmacro %}
