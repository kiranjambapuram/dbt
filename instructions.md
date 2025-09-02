# Instructions for dbt Job Trigger & Status Checker

This document provides instructions on how to set up a robust, two-part system in Snowflake to trigger a dbt Cloud job and monitor it to completion.

## Architecture Overview
... (section as before) ...

### Process Flow Diagram
... (section as before) ...

## 1. Prerequisites
... (section as before) ...

## 2. Component Setup
... (sections as before) ...

## 3. Automation Setup
... (sections as before) ...

## 4. Logging and Monitoring

This solution provides two levels of logging:

*   **Per-Record Logs:** The `sp_logs_trigger` and `sp_logs_job_status` columns in the `parameters` table contain a detailed, step-by-step log of the actions taken for that specific record. This is the best place for detailed debugging of a single request.
*   **SP Return Value:** The string returned by each stored procedure call contains a high-level summary of the entire batch run (e.g., "SP Started", "Found 5 records", "SP Finished"). This is useful for reviewing the overall health of the task runs in the Snowflake `query_history`.
