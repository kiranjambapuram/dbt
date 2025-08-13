CREATE OR REPLACE PROCEDURE check_dbt_job_status_sp()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python', 'requests', 'typing')
HANDLER = 'check_dbt_job_status'
EXTERNAL_ACCESS_INTEGRATIONS = (POC_EAI_TEST)
SECRETS = ('dbt_api_token' = POC_SECRET_TEST)
AS
$$
import snowflake.snowpark as snowpark
import requests
import json
from typing import List, Dict, Any
from _snowflake import get_secret_string

def execute_final_update(session: snowpark.Session, table: str, updates: dict, conditions: dict, logs: List[str]) -> None:
    """Helper function to execute a parameterized UPDATE statement and log the action."""
    set_clause = ", ".join([f'"{k.upper()}" = ?' for k in updates.keys()])
    where_clause = " AND ".join([f'"{k.upper()}" = ?' for k in conditions.keys()])
    sql = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
    params = list(updates.values()) + list(conditions.values())

    logs.append(f"Executing final status UPDATE: {sql} with params {params}")
    session.sql(sql, params=params).collect()
    logs.append("Final status UPDATE successful.")

def check_dbt_job_status(session: snowpark.Session) -> str:
    """
    Finds records with status 'S' (Submitted), polls the dbt Cloud API for their
    run status, and updates them to 'C' (Complete) or 'E' (Error).
    """
    logs = ["Status Checker SP execution started."]

    try:
        submitted_records_df = session.table("parameters").filter(snowpark.functions.col("status") == 'S')
        submitted_records = submitted_records_df.collect()

        if not submitted_records:
            logs.append("No submitted records to check. Exiting.")
            return "\n".join(logs)

        logs.append(f"Found {len(submitted_records)} submitted records to check.")

        dbt_api_token = get_secret_string('dbt_api_token')
        dbt_account_id = "YOUR_DBT_ACCOUNT_ID"
        headers = { "Authorization": f"Token {dbt_api_token}", "Content-Type": "application/json" }

        for record_row in submitted_records:
            record = record_row.as_dict()
            run_id = record.get("DBT_RUN_ID")

            composite_key = {
                "period": record["PERIOD"],
                "org_code": record["ORG_CODE"],
                "segment": record["SEGMENT"],
                "region": record["REGION"],
                "user": record["USER"],
                "timestamp": record["TIMESTAMP"]
            }

            if not run_id:
                logs.append(f"Skipping record {composite_key} because it has no dbt_run_id.")
                continue

            try:
                api_url = f"https://cloud.getdbt.com/api/v2/accounts/{dbt_account_id}/runs/{int(run_id)}/"
                logs.append(f"Checking status for run_id {run_id} at {api_url}")

                response = requests.get(api_url, headers=headers)
                response.raise_for_status()

                response_data = response.json().get("data", {})
                status_code = response_data.get("status")

                # dbt Cloud API Run Status Codes: 10=Success, 20=In Progress, 30=Error, 40=Cancelled
                if status_code == 10: # Success
                    logs.append(f"Run {run_id} Succeeded. Updating status to 'C'.")
                    execute_final_update(session, "parameters", {"status": 'C', "message": "dbt job completed successfully."}, composite_key, logs)
                elif status_code in [30, 40]: # Error or Cancelled
                    final_status = 'E' if status_code == 30 else 'Cancelled'
                    error_message = f"dbt job failed with status code: {status_code}"
                    logs.append(f"Run {run_id} Failed/Cancelled. Updating status to 'E'. Message: {error_message}")
                    execute_final_update(session, "parameters", {"status": 'E', "message": error_message}, composite_key, logs)
                else: # Still in progress or other state
                    logs.append(f"Run {run_id} is still in progress (status code: {status_code}). No update will be made.")

            except Exception as e:
                logs.append(f"An error occurred while checking run_id {run_id}: {str(e)}")
                continue

    except Exception as e:
        logs.append(f"A critical error occurred during the SP execution: {str(e)}")

    logs.append("Status Checker SP execution finished.")
    return "\n".join(logs)
$$;
