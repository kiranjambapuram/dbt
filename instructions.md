# Instructions for dbt Job Trigger & Status Checker

This document provides instructions on how to set up a robust, two-part system in Snowflake to trigger a dbt Cloud job and monitor it to completion.

## Architecture Overview

This solution uses a dynamic, configuration-driven approach.
1.  **Config Table (`dbt_job_config`):** A new table you create to map combinations of environment, segment, and region to specific dbt jobs and models.
2.  **Trigger SP (`trigger_dbt_job_sp`):** This procedure is triggered by new rows in the `parameters` table. It determines the current environment from the database name. For each new record, it looks up the correct `dbt_job_id` and `start_dbt_model` from the config table and triggers the corresponding job.
3.  **Status Checker SP (`check_dbt_job_status_sp`):** This procedure runs on a schedule, finds submitted jobs, polls the dbt API for the final status, and updates the `parameters` table.

### Process Flow Diagram
```mermaid
graph TD
    subgraph "Part 1: Triggering"
        A[User INSERTs new row(s)] --> B(parameters_stream);
        B --> C{trigger_task};
        C -- runs WHEN stream has data --> D[CALL trigger_dbt_job_sp];
        D --> E{SP: Get Environment & ALL 'N' records};
        E --> F{Loop through records};
        F --> G{For each record: Query dbt_job_config};
        G --> H{Lock record, Call dynamic dbt job, Set status='S'};
    end

    subgraph "Part 2: Monitoring"
        I{checker_task} -- runs every 2 minutes --> J[CALL check_dbt_job_status_sp];
        J --> K{Find records where status = 'S'};
        K --> L{Poll dbt API with dbt_run_id};
        L -- Succeeded --> M[Update status to 'C'];
        L -- Failed --> N[Update status to 'E'];
    end
```

## 1. Prerequisites

### 1.1. `parameters` Table
This table stores your job requests and their statuses.
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
    queued_duration VARCHAR
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
This solution requires a pre-existing External Access Integration and a Secret. The provided scripts are pre-configured to use `POC_EAI_TEST` and `POC_SECRET_TEST`.

## 2. Component Setup

### 2.1. Create the Trigger Stored Procedure
The file `create_dbt_trigger_sp.sql` contains the script for the first SP. Before running, verify the integration/secret names and update the `YOUR_DBT_ACCOUNT_ID` placeholder. Then, execute the script in Snowflake to create the `trigger_dbt_job_sp` procedure.

### 2.2. Create the Status Checker Stored Procedure
The file `create_status_checker_sp.sql` contains the script for the second SP. Verify the integration/secret names and update the `YOUR_DBT_ACCOUNT_ID` placeholder. Then, execute the script in Snowflake to create the `check_dbt_job_status_sp` procedure.

## 3. Automation Setup

### 3.1. Create the Stream for the Trigger
```sql
CREATE OR REPLACE STREAM parameters_stream ON TABLE parameters;
```

### 3.2. Create and Schedule the Tasks
**Task 1: The Trigger Task (Event-Driven)**
This task calls `trigger_dbt_job_sp` (now with no parameters). A single run will process all available records based on the lookup table.
```sql
CREATE OR REPLACE TASK dbt_trigger_task
  WAREHOUSE = 'YOUR_WAREHOUSE'
  SCHEDULE = '1 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('parameters_stream')
AS
  CALL trigger_dbt_job_sp();
```

**Task 2: The Status Checker Task (Scheduled)**
This task remains the same, calling `check_dbt_job_status_sp` on a regular schedule.
```sql
CREATE OR REPLACE TASK dbt_status_checker_task
  WAREHOUSE = 'YOUR_WAREHOUSE'
  SCHEDULE = '2 MINUTE'
AS
  CALL check_dbt_job_status_sp();
```

### 3.3. Start the Tasks
```sql
ALTER TASK dbt_trigger_task RESUME;
ALTER TASK dbt_status_checker_task RESUME;
```
Your automated pipeline is now active. Inserting rows into the `parameters` table will kick off the entire dynamic process.
