import snowflake.snowpark as snowpark
import requests
import json
from typing import Tuple, Dict, Any, Optional, List
from _snowflake import get_secret_string

def execute_update(session: snowpark.Session, table: str, updates: dict, conditions: dict, logs: List[str]) -> None:
    """Helper function to execute a parameterized UPDATE statement and log the action."""
    set_clause = ", ".join([f'"{k.upper()}" = ?' for k in updates.keys()])
    where_clause = " AND ".join([f'"{k.upper()}" = ?' for k in conditions.keys()])
    sql = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
    params = list(updates.values()) + list(conditions.values())

    logs.append(f"Executing UPDATE: {sql} with params {params}")
    session.sql(sql, params=params).collect()
    logs.append("UPDATE successful.")

def get_and_lock_next_record(session: snowpark.Session, logs: List[str]) -> Optional[Tuple[Dict[str, Any], Dict[str, Any]]]:
    """
    Finds, locks, and returns the next available record, while logging actions.
    """
    logs.append("Searching for a new record (status='N')...")
    record_to_process_df = session.table("parameters").filter(snowpark.functions.col("status") == 'N').order_by("timestamp").limit(1)
    records = record_to_process_df.collect()

    if not records:
        logs.append("No new records found.")
        return None

    record = records[0].as_dict()
    logs.append(f"Found record: {record}")

    composite_key = {
        "period": record["PERIOD"],
        "org_code": record["ORG_CODE"],
        "segment": record["SEGMENT"],
        "region": record["REGION"],
        "user": record["USER"],
        "timestamp": record["TIMESTAMP"]
    }

    logs.append(f"Locking record by setting status to 'P'.")
    execute_update(session, "parameters", {"status": 'P'}, composite_key, logs)

    return record, composite_key

def trigger_dbt_job(session: snowpark.Session, dbt_job_id: int) -> str:
    """
    The main handler function that triggers a dbt Cloud job and updates its status to 'S' (Submitted).
    """
    logs = ["SP execution started."]

    try:
        result = get_and_lock_next_record(session, logs)
        if result is None:
            logs.append("SP execution finished: No new records to process.")
            return "\n".join(logs)
        record, composite_key = result

    except Exception as e:
        logs.append(f"CRITICAL ERROR during record selection/locking phase: {str(e)}")
        return "\n".join(logs)

    try:
        logs.append("Fetching dbt API token from secret...")
        dbt_api_token = get_secret_string('dbt_api_token')
        logs.append("Token fetched successfully.")

        dbt_account_id = "YOUR_DBT_ACCOUNT_ID"
        api_url = f"https://cloud.getdbt.com/api/v2/accounts/{dbt_account_id}/jobs/{dbt_job_id}/run/"
        logs.append(f"Preparing API call to {api_url}")

        dbt_vars = {
            "reporting_period": record["PERIOD"],
            "ps_org_code": record["ORG_CODE"],
            "ps_segment": record["SEGMENT"],
            "ps_region": record["REGION"]
        }
        vars_json_string = json.dumps(dbt_vars)
        steps_override_command = f"dbt run -s fct_foundation+ --vars '{vars_json_string}'"

        headers = { "Authorization": f"Token {dbt_api_token}", "Content-Type": "application/json" }
        payload = {
            "cause": f"Triggered by Snowflake SP for period {record['PERIOD']}",
            "schema_override": f"dbt_cloud_{record['ORG_CODE']}_{record['SEGMENT']}_{record['REGION']}",
            "steps_override": [steps_override_command]
        }
        logs.append(f"Payload: {json.dumps(payload)}")

        response = requests.post(api_url, headers=headers, data=json.dumps(payload))
        logs.append(f"API Response Status Code: {response.status_code}")
        response.raise_for_status()

        response_data = response.json()
        run_id = response_data.get("data", {}).get("id")

        if run_id is None:
            raise Exception("Could not find 'id' in dbt API response.")

        success_message = f"API call successful. dbt run initiated with run_id: {run_id}."
        logs.append(success_message)

        # On success, update status to 'S' and store the run_id
        execute_update(session, "parameters", {"status": 'S', "dbt_run_id": run_id, "message": success_message}, composite_key, logs)
        logs.append(f"SUCCESS: Record submitted for processing. Final Status: 'S'.")

    except Exception as e:
        error_message = str(e).replace("'", "''")
        logs.append(f"ERROR: An exception occurred: {error_message}")
        # On failure, update status to 'E'
        execute_update(session, "parameters", {"status": 'E', "message": error_message}, composite_key, logs)
        logs.append(f"FAILURE: Record processing failed. Final Status: 'E'.")

    return "\n".join(logs)
