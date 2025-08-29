# Instructions for dbt Job Trigger & Status Checker

This document provides instructions on how to set up a robust, two-part system in Snowflake to trigger a dbt Cloud job and monitor it to completion.

## Architecture Overview

This solution uses a dynamic, configuration-driven approach.
1.  **Config Table (`dbt_job_config`):** A new table you create to map combinations of environment, segment, and region to specific dbt jobs and models.
2.  **Trigger SP (`trigger_dbt_job_sp`):** This procedure is triggered by new rows in the `parameters` table. It determines the current environment from the database name. For each new record, it looks up the correct `dbt_job_id` and `start_dbt_model` from the config table, triggers the corresponding job, and writes its execution log to the `sp_logs_trigger` column.
3.  **Status Checker SP (`check_dbt_job_status_sp`):** This procedure runs on a schedule, finds submitted jobs, polls the dbt API for the final status, and updates the `parameters` table, including writing its execution log to the `sp_logs_job_status` column.

### Process Flow Diagram
... (Diagram will be updated later if necessary) ...

## 1. Prerequisites

### 1.1. `parameters` Table
Create the table that will store your job requests and their statuses.
```sql
CREATE OR REPLACE TABLE parameters (
    period VARCHAR(6),
    org_code VARCHAR,
    segment VARCHAR,
    region VARCHAR,
    "user" VARCHAR,
    "timestamp" TIMESTAMP_NTZ(9),
    status VARCHAR DEFAULT 'N',
    message VARCHAR,
    dbt_run_id INTEGER,
    started_on TIMESTAMP_NTZ(9),
    ended_on TIMESTAMP_NTZ(9),
    duration VARCHAR,
    queued_duration VARCHAR,
    sp_logs_trigger VARCHAR,
    sp_logs_job_status VARCHAR
);
```

### 1.2. `dbt_job_config` Table (New)
This new table acts as a lookup to make the process dynamic.
```sql
CREATE OR REPLACE TABLE dbt_job_config (
    environment VARCHAR,
    segment VARCHAR,
    region VARCHAR,
    start_dbt_model VARCHAR,
    dbt_job_id INTEGER
);

-- Example: Insert a configuration record
INSERT INTO dbt_job_config (environment, segment, region, start_dbt_model, dbt_job_id)
VALUES ('dev', 'Enterprise', 'US_EAST', 'fct_foundation+', 12345);
```

### 1.3. External Access Integration and Secret
... (section as before) ...

## 2. Component Setup
... (sections as before) ...

## 3. Automation Setup
... (sections as before) ...
