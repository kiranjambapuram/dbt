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
from typing import List, Dict, Any, Optional
from _snowflake import get_secret_string

def execute_final_update(session: snowpark.Session, table: str, updates: dict, conditions: dict) -> None:
    """Helper function to execute a parameterized UPDATE statement."""
    final_updates = {k: v for k, v in updates.items() if v is not None}
    if not final_updates:
        return

    set_clause = ", ".join([f'"{k.upper()}" = ?' for k in final_updates.keys()])
    where_clause = " AND ".join([f'"{k.upper()}" = ?' for k in conditions.keys()])
    sql = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
    params = list(final_updates.values()) + list(conditions.values())
    session.sql(sql, params=params).collect()

def check_dbt_job_status(session: snowpark.Session) -> str:
    """
    Finds 'S' records, polls the dbt API, and updates them, logging the details to the row.
    """
    sp_logs = ["Status Checker SP execution started."]

    try:
        submitted_records_df = session.table("parameters").filter(snowpark.functions.col("status") == 'S')
        submitted_records = submitted_records_df.collect()

        if not submitted_records:
            sp_logs.append("No submitted records to check. Exiting.")
            return "\n".join(sp_logs)

        sp_logs.append(f"Found {len(submitted_records)} submitted records to check.")

        dbt_api_token = get_secret_string('dbt_api_token')
        dbt_account_id = "YOUR_DBT_ACCOUNT_ID"
        headers = { "Authorization": f"Token {dbt_api_token}", "Content-Type": "application/json" }

        for i, record_row in enumerate(submitted_records):
            record = record_row.as_dict()
            record_logs = []
            run_id = record.get("DBT_RUN_ID")
            composite_key = {"period": record["PERIOD"], "org_code": record["ORG_CODE"], "segment": record["SEGMENT"], "region": record["REGION"], "user": record["USER"], "timestamp": record["TIMESTAMP"]}
            sp_logs.append(f"\nChecking record {i+1}/{len(submitted_records)}: {composite_key}")

            if not run_id:
                record_logs.append(f"Skipping record because it has no dbt_run_id.")
                update_payload = {"status": 'E', "message": "Missing dbt_run_id.", "sp_logs_job_status": "\n".join(record_logs)}
                execute_final_update(session, "parameters", update_payload, composite_key)
                continue

            try:
                api_url = f"https://cloud.getdbt.com/api/v2/accounts/{dbt_account_id}/runs/{int(run_id)}/"
                record_logs.append(f"Checking status for run_id {run_id} at {api_url}")

                response = requests.get(api_url, headers=headers)
                response.raise_for_status()

                response_data = response.json().get("data", {})
                record_logs.append(f"API Response: {response_data}")

                if response_data.get("is_complete"):
                    record_logs.append(f"Run {run_id} is complete.")

                    update_payload = {
                        "started_on": response_data.get("created_at"),
                        "ended_on": response_data.get("finished_at"),
                        "duration": response_data.get("run_duration"),
                        "queued_duration": response_data.get("queued_duration")
                    }

                    if response_data.get("is_success"):
                        record_logs.append(f"Run {run_id} Succeeded. Updating status to 'C'.")
                        update_payload["status"] = 'C'
                        update_payload["message"] = "dbt job completed successfully."
                    else:
                        error_message = f"dbt job finished with failure. Final status: {response_data.get('status_humanized', 'Unknown')}"
                        record_logs.append(f"Run {run_id} Failed. Updating status to 'E'.")
                        update_payload["status"] = 'E'
                        update_payload["message"] = error_message

                    update_payload["sp_logs_job_status"] = "\n".join(record_logs)
                    execute_final_update(session, "parameters", update_payload, composite_key)
                else:
                    sp_logs.append(f"Run {run_id} is still in progress. No update will be made on this cycle.")

            except Exception as e:
                error_message = f"An error occurred while checking run_id {run_id}: {str(e)}"
                sp_logs.append(error_message)
                record_logs.append(error_message)
                update_payload = {"status": 'E', "message": error_message, "sp_logs_job_status": "\n".join(record_logs)}
                execute_final_update(session, "parameters", update_payload, composite_key)
                continue

    except Exception as e:
        sp_logs.append(f"A critical error occurred during the SP execution: {str(e)}")

    sp_logs.append("\nStatus Checker SP execution finished.")
    return "\n".join(sp_logs)
$$;
