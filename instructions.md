# Instructions for dbt Job Trigger & Status Checker

This document provides instructions on how to set up a robust, two-part system in Snowflake to trigger a dbt Cloud job and monitor it to completion.

## Architecture Overview

This solution consists of two stored procedures and two tasks:
1.  **Trigger SP (`trigger_dbt_job_sp`):** This procedure is triggered when new rows appear in the `parameters` table. It reads **all** available new records (status 'N') and processes them one-by-one in a loop within a single run. For each record, it calls the dbt Cloud API, updates the status to 'S' (Submitted), and saves the `run_id`.
2.  **Status Checker SP (`check_dbt_job_status_sp`):** This procedure runs on a schedule. It finds all 'S' records and polls the dbt Cloud API to check the final status of the run. It then updates the row's status to 'C' (Complete) or 'E' (Error) and records the job's start time, end time, duration, and queue duration.

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
        K -- Succeeded --> L[Update status to 'C', started_on, ended_on, duration, queue_duration];
        K -- Failed --> M[Update status to 'E', started_on, ended_on, duration, queue_duration];
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
    queue_duration INTEGER
);
```

### 1.2. External Access Integration and Secret
This solution requires a pre-existing External Access Integration and a Secret to store your dbt Cloud API token. The provided scripts are pre-configured to use an integration named `POC_EAI_TEST` and a secret named `POC_SECRET_TEST`.

## 2. Component Setup
... (sections 2.1 and 2.2 as before) ...

## 3. Running the Process

### 3.1. Insert Data into the `parameters` Table
Insert a new row into the table to trigger the process. The `status` will default to 'N' automatically.

**Important:** For accurate `queue_duration` calculation, the `timestamp` column should be populated with a UTC timestamp. Use `SYSTIMESTAMP()` or `CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())` instead of `CURRENT_TIMESTAMP()`.

```sql
-- Example: Insert a new record for processing using a UTC timestamp
INSERT INTO parameters (period, org_code, segment, region, "user", "timestamp")
VALUES ('202408', 'ORG123', 'ENTERPRISE', 'US_EAST', 'jules_dev', SYSTIMESTAMP());
```

### 3.2. Manual Trigger (for testing)
You can call the stored procedures manually. Note that this bypasses the automated Stream/Task setup.
```sql
-- Trigger a job (replace 12345 with your dbt Job ID)
CALL trigger_dbt_job_sp(12345);

-- Check for the status of completed jobs
CALL check_dbt_job_status_sp();
```

## 4. Automation Setup (Recommended)
... (sections 4.1, 4.2, 4.3 as before) ...
