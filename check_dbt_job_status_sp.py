import snowflake.snowpark as snowpark
import requests
import json
import re
from typing import List, Dict, Any, Optional
from _snowflake import get_secret_string

def parse_iso8601_duration(duration_str: Optional[str]) -> Optional[int]:
    """
    Parses an ISO 8601 duration string (e.g., 'PT1M30.5S') into total seconds.
    Returns None if the input is None or invalid.
    """
    if not duration_str:
        return None

    # This regex handles the P...T...S format from dbt Cloud
    match = re.search(r"T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?", duration_str)
    if not match:
        return None

    hours = float(match.group(1)) if match.group(1) else 0
    minutes = float(match.group(2)) if match.group(2) else 0
    seconds = float(match.group(3)) if match.group(3) else 0

    return int(hours * 3600 + minutes * 60 + seconds)

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
    Finds records with status 'S', polls the dbt API, and updates them based on the outcome.
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
            composite_key = {"period": record["PERIOD"], "org_code": record["ORG_CODE"], "segment": record["SEGMENT"], "region": record["REGION"], "user": record["USER"], "timestamp": record["TIMESTAMP"]}

            if not run_id:
                logs.append(f"Skipping record {composite_key} because it has no dbt_run_id.")
                continue

            try:
                api_url = f"https://cloud.getdbt.com/api/v2/accounts/{dbt_account_id}/runs/{int(run_id)}/"
                logs.append(f"Checking status for run_id {run_id} at {api_url}")

                response = requests.get(api_url, headers=headers)
                response.raise_for_status()

                response_data = response.json().get("data", {})

                if response_data.get("is_complete"):
                    logs.append(f"Run {run_id} is complete.")

                    update_payload = {
                        "started_on": response_data.get("created_at"),
                        "ended_on": response_data.get("finished_at"),
                        "duration": parse_iso8601_duration(response_data.get("run_duration")),
                        "queue_duration": parse_iso8601_duration(response_data.get("queued_duration"))
                    }

                    if response_data.get("is_success"):
                        logs.append(f"Run {run_id} Succeeded. Updating status to 'C'.")
                        update_payload["status"] = 'C'
                        update_payload["message"] = "dbt job completed successfully."
                    else:
                        error_message = f"dbt job finished with failure. Final status: {response_data.get('status_humanized', 'Unknown')}"
                        logs.append(f"Run {run_id} Failed. Updating status to 'E'. Message: {error_message}")
                        update_payload["status"] = 'E'
                        update_payload["message"] = error_message

                    execute_final_update(session, "parameters", update_payload, composite_key, logs)
                else:
                    logs.append(f"Run {run_id} is still in progress. No update will be made.")

            except Exception as e:
                logs.append(f"An error occurred while checking run_id {run_id}: {str(e)}")
                continue

    except Exception as e:
        logs.append(f"A critical error occurred during the SP execution: {str(e)}")

    logs.append("Status Checker SP execution finished.")
    return "\n".join(logs)
