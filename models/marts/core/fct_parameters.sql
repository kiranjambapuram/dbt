-- This model conditionally loads data from stg_parameters based on a master lock.

-- Get the period to check from a dbt variable.
{% set period_to_check = var('run_period') %}

-- Check if the master lock is on for the given period
{% if is_master_lock_on(period_to_check) == '1' %}

    -- If the lock is ON, select no records from the source table.
    -- This ensures the model runs successfully but produces an empty table,
    -- effectively pausing data flow for the locked period.
    select * from {{ ref('stg_parameters') }} where 1=0

{% else %}

    -- If the lock is OFF, select all records from the source table.
    select * from {{ ref('stg_parameters') }}

{% endif %}
