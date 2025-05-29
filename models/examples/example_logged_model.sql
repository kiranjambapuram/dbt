-- models/examples/example_logged_model.sql

{{
    config(
        materialized='table',
        pre_hook="{{ log_run_start(message='Starting example_logged_model execution.') }}",
        post_hook="{{ log_run_end(status=adapter.get_status(model.status)) }}"
    )
}}

{{ log_step_event(step_name='generate_initial_data', step_sequence_num=1, message='Generating a simple dataset with one ID.') }}
WITH source_data AS (
    SELECT 1 as id, 'example_value' as data_field
),

{{ log_step_event(step_name='apply_transformation', step_sequence_num=2, message='Applying a minor transformation (selecting all).') }}
transformed_data AS (
    SELECT * FROM source_data
),

{{ log_step_event(step_name='final_notes', message='Model processing complete before final select. No sequence number provided.') }}
-- This step intentionally omits step_sequence_num to demonstrate it's optional.
final_step_before_select AS (
    SELECT *, 'extra_note' as note FROM transformed_data
)

-- Final selection for the model
SELECT id, data_field, note FROM final_step_before_select
