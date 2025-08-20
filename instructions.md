# Instructions for dbt Job Trigger & Status Checker

This document provides instructions on how to set up a robust, two-part system in Snowflake to trigger a dbt Cloud job and monitor it to completion.

## Architecture Overview

This solution consists of two stored procedures and two tasks:
1.  **Trigger SP (`trigger_dbt_job_sp`):** This procedure is triggered when new rows appear in the `parameters` table. It reads **all** available new records (status 'N') and processes them one-by-one in a loop within a single run. For each record, it calls the dbt Cloud API, updates the status to 'S' (Submitted), and saves the `run_id`.
2.  **Status Checker SP (`check_dbt_job_status_sp`):** This procedure runs on a schedule. It finds all 'S' records and polls the dbt Cloud API to check the final status of the run. It then updates the row's status to 'C' (Complete) or 'E' (Error) and records the job's start time, end time, duration, and queue time.

### Process Flow Diagram
```mermaid
graph TD
    subgraph "Part 1: Triggering"
        A[User INSERTs new row(s)] --> B(parameters_stream);
        B --> C{trigger_task};
        C -- runs ONCE when stream has data --> D[CALL trigger_dbt_job_sp];
        D --> E{SP: Get ALL 'N' records};
        E --> F{Loop through records};
        F --> G{For each record: Lock, Call API, Set status='S'};
    end

    subgraph "Part 2: Monitoring"
        H{checker_task} -- runs every 2 minutes --> I[CALL check_dbt_job_status_sp];
        I --> J{Find records where status = 'S'};
        J --> K{Poll dbt API with dbt_run_id};
        K -- Succeeded --> L[Update status to 'C', started_on, ended_on, duration, queue_time];
        K -- Failed --> M[Update status to 'E', started_on, ended_on, duration, queue_time];
    end
```

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
    duration INTEGER,
    queue_time INTEGER
);
```

### 1.2. External Access Integration and Secret
This solution requires a pre-existing External Access Integration and a Secret to store your dbt Cloud API token. The provided scripts are pre-configured to use an integration named `POC_EAI_TEST` and a secret named `POC_SECRET_TEST`.

## 2. Component Setup

### 2.1. Create the Trigger Stored Procedure
The file `create_dbt_trigger_sp.sql` contains the script for the first SP. Before running, verify the integration/secret names and update the `YOUR_DBT_ACCOUNT_ID` placeholder. Then, execute the script in Snowflake to create the `trigger_dbt_job_sp` procedure.

### 2.2. Create the Status Checker Stored Procedure
The file `create_status_checker_sp.sql` contains the script for the second SP. Verify the integration/secret names and update the `YOUR_DBT_ACCOUNT_ID` placeholder. Then, execute the script in Snowflake to create the `check_dbt_job_status_sp` procedure.

## 3. Automation Setup

### 3.1. Create the Stream for the Trigger
This stream will detect new rows in the `parameters` table.
```sql
CREATE OR REPLACE STREAM parameters_stream ON TABLE parameters;
```

### 3.2. Create and Schedule the Tasks

You need two separate tasks, one for each stored procedure.

**Task 1: The Trigger Task (Event-Driven)**
This task calls `trigger_dbt_job_sp` once when new records appear in the stream. A single run of the SP will process all available records.
```sql
-- Replace 12345 with the dbt Job ID you want this trigger to run
CREATE OR REPLACE TASK dbt_trigger_task
  WAREHOUSE = 'YOUR_WAREHOUSE' -- Replace with your warehouse
  SCHEDULE = '1 MINUTE'
  WHEN SYSTEM$STREAM_HAS_DATA('parameters_stream')
AS
  CALL trigger_dbt_job_sp(12345);
```

**Task 2: The Status Checker Task (Scheduled)**
This task calls `check_dbt_job_status_sp` on a regular schedule to poll for results.
```sql
CREATE OR REPLACE TASK dbt_status_checker_task
  WAREHOUSE = 'YOUR_WAREHOUSE' -- Replace with your warehouse
  SCHEDULE = '2 MINUTE' -- Runs every 2 minutes
AS
  CALL check_dbt_job_status_sp();
```

### 3.3. Start the Tasks
Activate both tasks to complete the setup.
```sql
ALTER TASK dbt_trigger_task RESUME;
ALTER TASK dbt_status_checker_task RESUME;
```

Your automated pipeline is now active. Inserting rows into the `parameters` table will kick off the entire process.
