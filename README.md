Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](http://slack.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

### Advanced Audit Logging System

This project implements a two-table audit logging system designed to provide both high-level run summaries and granular, step-by-step event details within your dbt models.

#### Audit Tables

Two primary tables are used for logging:

1.  **`{{ target.schema }}.dbt_audit_summary`**:
    *   **Purpose**: Stores one record per dbt model run (or a significant, named process), summarizing its overall execution.
    *   **Schema**:
        *   `invocation_id VARCHAR(255)`: The unique identifier for the dbt invocation.
        *   `model_name VARCHAR(255)`: The name of the dbt model or process.
        *   `run_started_at TIMESTAMP`: Timestamp when the model/process execution started.
        *   `run_ended_at TIMESTAMP NULL`: Timestamp when the model/process execution finished.
        *   `execution_time_seconds INTEGER NULL`: Total execution time in seconds.
        *   `rows_processed INTEGER NULL`: Number of rows processed by the model (currently not automatically populated by these macros).
        *   `status VARCHAR(50)`: The final status of the model/process run (e.g., 'started', 'completed', 'failed').
        *   `run_by_user VARCHAR(255)`: The user who executed the dbt run.
        *   `custom_message VARCHAR(1000) NULL`: An optional, user-provided message for additional context about the overall run.
        *   `PRIMARY KEY (invocation_id, model_name)`

2.  **`{{ target.schema }}.dbt_audit_detail`**:
    *   **Purpose**: Stores records for granular events or steps within a model's execution, allowing for detailed tracing.
    *   **Schema**:
        *   `detail_log_id VARCHAR(255) PRIMARY KEY`: A unique identifier for the detail log entry.
        *   `invocation_id VARCHAR(255)`: Links to `dbt_audit_summary.invocation_id`.
        *   `model_name VARCHAR(255)`: The name of the model associated with this event.
        *   `step_name VARCHAR(255)`: A descriptive name for the specific step or event.
        *   `step_sequence_num INTEGER NULL`: Optional, user-provided sequence number for ordering steps.
        *   `event_timestamp TIMESTAMP`: Timestamp when the specific event occurred.
        *   `status VARCHAR(50)`: Status of the event (e.g., 'info', 'debug', 'warning', 'error').
        *   `message VARCHAR(1000) NULL`: A detailed message for this specific event.

#### Macro Usage

This system relies on a set of macros to manage table creation and logging:

1.  **`create_dbt_audit_summary_table()`** (in `macros/ensure_audit_summary_table.sql`)
    *   **Purpose**: Creates/ensures the `{{ target.schema }}.dbt_audit_summary` table.
    *   **Optimization**: Uses `flags.AUDIT_SUMMARY_TABLE_ENSURED` to run DDL only once per `dbt run`.
    *   **Usage**: Called automatically by `log_run_start()`. Can be used in `on-run-start` for explicit setup if needed by other processes.

2.  **`create_dbt_audit_detail_table()`** (in `macros/ensure_audit_detail_table.sql`)
    *   **Purpose**: Creates/ensures the `{{ target.schema }}.dbt_audit_detail` table.
    *   **Optimization**: Uses `flags.AUDIT_DETAIL_TABLE_ENSURED` to run DDL only once per `dbt run`.
    *   **Usage**: Called automatically by `log_step_event()`.

3.  **`log_run_start(model_name_arg=None, message=None)`** (in `macros/log_run_start.sql`)
    *   **Purpose**: Logs the beginning of a model run or a significant process. Inserts a record into `dbt_audit_summary` with `status='started'`.
    *   **Parameters**:
        *   `model_name_arg` (string, optional): Name of the model/process. Defaults to `this.name` (current model's name).
        *   `message` (string, optional): Custom message for the summary record.
    *   **Usage**: Typically used in a model's `pre-hook`.
        ```sql
        -- In your_model.sql
        {{ config(pre_hook="{{ log_run_start(message='Initial data load starting.') }}") }}
        ```

4.  **`log_run_end(model_name_arg=None, status='completed')`** (in `macros/log_run_end.sql`)
    *   **Purpose**: Logs the end of a model run. Updates the corresponding record in `dbt_audit_summary` with `run_ended_at`, `execution_time_seconds`, and the final `status`.
    *   **Parameters**:
        *   `model_name_arg` (string, optional): Name of the model/process. Defaults to `this.name`.
        *   `status` (string, optional): Final status (e.g., 'completed', 'failed'). Defaults to 'completed'.
    *   **Usage**: **Crucially important to use in a `post-hook` to accurately capture the run status, including failures.**
        ```sql
        -- In your_model.sql
        {{
            config(
                post_hook = "{{ log_run_end(status=adapter.get_status(model.status)) }}"
            )
        }}
        ```
        The `adapter.get_status(model.status)` dynamically captures the actual execution status of the model.

5.  **`log_step_event(step_name, message=None, status='info', model_name_arg=None, step_sequence_num=None)`** (in `macros/log_step_event.sql`)
    *   **Purpose**: Logs a granular event or step within a model's execution to `dbt_audit_detail`.
    *   **Parameters**:
        *   `step_name` (string): Descriptive name for the step.
        *   `message` (string, optional): Detailed message for this event. Defaults to `NULL`.
        *   `status` (string, optional): Status of this event (e.g., 'info', 'debug', 'error'). Defaults to 'info'.
        *   `model_name_arg` (string, optional): Name of the model/process. Defaults to `this.name`.
        *   `step_sequence_num` (integer, optional): A user-defined sequence number for the step. Defaults to `NULL`. Useful for explicit ordering of logged steps.
    *   **Usage**: Called directly within your model's SQL at relevant points.

#### Example Model Structure

Here's how you might structure a model to use these logging macros:

```sql
-- models/my_complex_model.sql
{{
    config(
        pre_hook = "{{ log_run_start(message='Starting execution of my_complex_model.') }}",
        post_hook = "{{ log_run_end(status=adapter.get_status(model.status)) }}"
    )
}}

{{ log_step_event(step_name='cte_intermediate_data_prep', step_sequence_num=1, message='Preparing intermediate customer and order data.') }}
WITH intermediate_data AS (
    SELECT
        c.id as customer_id,
        c.name as customer_name,
        o.order_id,
        o.order_date,
        o.amount
    FROM {{ ref('stg_customers') }} c
    JOIN {{ ref('stg_orders') }} o ON c.id = o.customer_id
    WHERE o.order_date >= '2023-01-01'
),

{{ log_step_event(step_name='cte_aggregated_results', message='Aggregating data per customer.') }}
aggregated_results AS (
    SELECT
        customer_id,
        customer_name,
        COUNT(order_id) as number_of_orders,
        SUM(amount) as total_revenue
    FROM intermediate_data
    GROUP BY 1, 2
)

{{ log_step_event(step_name='cte_aggregated_results', step_sequence_num=2, message='Aggregating data per customer.') }}
aggregated_results AS (
    SELECT
        customer_id,
        customer_name,
        COUNT(order_id) as number_of_orders,
        SUM(amount) as total_revenue
    FROM intermediate_data
    GROUP BY 1, 2
)

{{ log_step_event(step_name='final_selection', step_sequence_num=3, message='Final selection and applying business rules.') }}
SELECT
    ar.customer_name,
    ar.number_of_orders,
    ar.total_revenue,
    CASE
        WHEN ar.total_revenue > 1000 THEN 'High Value'
        WHEN ar.total_revenue > 500 THEN 'Medium Value'
        ELSE 'Low Value'
    END as customer_segment
FROM aggregated_results ar
ORDER BY ar.total_revenue DESC
```

#### Querying the Audit Logs

*   **Overall Run Summaries**:
    ```sql
    SELECT * FROM {{ target.schema }}.dbt_audit_summary
    WHERE model_name = 'my_complex_model'
    ORDER BY run_started_at DESC;
    ```
*   **Detailed Steps for a Specific Run**:
    ```sql
    SELECT
        s.invocation_id,
        s.model_name,
        s.run_started_at as summary_run_started_at,
        s.status as summary_status,
        d.step_name,
        d.step_sequence_num,
        d.event_timestamp,
        d.status as step_status,
        d.message
    FROM {{ target.schema }}.dbt_audit_summary s
    JOIN {{ target.schema }}.dbt_audit_detail d
      ON s.invocation_id = d.invocation_id AND s.model_name = d.model_name
    WHERE s.model_name = 'my_complex_model' AND s.invocation_id = 'your_specific_invocation_id' -- Replace with an actual invocation_id
    ORDER BY d.step_sequence_num ASC, d.event_timestamp ASC;
    ```

#### Testing the Logging

The `models/examples/example_logged_model.sql` is set up to use the `log_run_start` and `log_run_end` macros. You can run it and query the `dbt_audit_summary` table as shown above to verify.
To test `log_step_event`, you would need to add calls to it within a model's SQL, similar to the `my_complex_model.sql` example.

#### Limitations

*   **`rows_processed` in `dbt_audit_summary`**: This is not automatically populated by the current macros. Capturing this accurately usually requires database-specific queries or parsing `run_results.json` via an `on-run-end` hook.
*   **Error Messages in `dbt_audit_summary`**: While `log_run_end` captures the 'failed' status via `adapter.get_status(model.status)`, detailed error messages from dbt are not automatically logged into the `custom_message` field by these macros. This would also typically require an `on-run-end` hook and parsing of `run_results.json`.
```
