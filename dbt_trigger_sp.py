import snowflake.snowpark as snowpark
import requests
import json
from typing import List
from _snowflake import get_secret_string

def execute_update(session: snowpark.Session, table: str, updates: dict, conditions: dict, logs: List[str]) -> None:
    """Helper function to execute a parameterized UPDATE statement and log the action."""
    set_clause = ", ".join([f'"{k.upper()}" = ?' for k in updates.keys()])
    where_clause = " AND ".join([f'"{k.upper()}" = ?' for k in conditions.keys()])
    sql = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
    params = list(updates.values()) + list(conditions.values())

    logs.append(f"  - Executing UPDATE: {sql} with params {params}")
    session.sql(sql, params=params).collect()
    logs.append("  - UPDATE successful.")

def trigger_dbt_job(session: snowpark.Session, dbt_job_id: int) -> str:
    """
    The main handler function that triggers dbt Cloud jobs in a batch.
    It fetches all 'N' records, then loops through them one by one.
    """
    logs = ["SP execution started: Batch mode."]

    try:
        logs.append("Selecting all new records (status='N')...")
        records_to_process_df = session.table("parameters").filter(snowpark.functions.col("status") == 'N').order_by("timestamp")
        records = records_to_process_df.collect()

        if not records:
            logs.append("No new records to process. Exiting.")
            return "\n".join(logs)

        logs.append(f"Found {len(records)} records to process.")

        dbt_api_token = get_secret_string('dbt_api_token')
        dbt_account_id = "YOUR_DBT_ACCOUNT_ID"
        headers = { "Authorization": f"Token {dbt_api_token}", "Content-Type": "application/json" }
        api_url = f"https://cloud.getdbt.com/api/v2/accounts/{dbt_account_id}/jobs/{dbt_job_id}/run/"

        for i, record_row in enumerate(records):
            record = record_row.as_dict()
            composite_key = {
                "period": record["PERIOD"],
                "org_code": record["ORG_CODE"],
                "segment": record["SEGMENT"],
                "region": record["REGION"],
                "user": record["USER"],
                "timestamp": record["TIMESTAMP"]
            }
            logs.append(f"\nProcessing record {i+1}/{len(records)}: {composite_key}")

            try:
                # Lock record
                execute_update(session, "parameters", {"status": 'P'}, composite_key, logs)

                # Prepare and trigger API call
                dbt_vars = {
                    "reporting_period": record["PERIOD"],
                    "ps_org_code": record["ORG_CODE"],
                    "ps_segment": record["SEGMENT"],
                    "ps_region": record["REGION"]
                }
                vars_json_string = json.dumps(dbt_vars)
                steps_override_command = f"dbt run -s fct_foundation+ --vars '{vars_json_string}'"

                payload = {
                    "cause": f"Triggered by Snowflake SP for period {record['PERIOD']}",
                    "schema_override": f"dbt_cloud_{record['ORG_CODE']}_{record['SEGMENT']}_{record['REGION']}",
                    "steps_override": [steps_override_command]
                }
                logs.append(f"  - Payload: {json.dumps(payload)}")

                response = requests.post(api_url, headers=headers, data=json.dumps(payload))
                logs.append(f"  - API Response Status Code: {response.status_code}")
                response.raise_for_status()

                response_data = response.json()
                run_id = response_data.get("data", {}).get("id")

                if run_id is None:
                    raise Exception("Could not find 'id' in dbt API response.")

                success_message = f"API call successful. dbt run initiated with run_id: {run_id}."
                logs.append(f"  - {success_message}")

                # Update status to 'S'
                execute_update(session, "parameters", {"status": 'S', "dbt_run_id": run_id, "message": success_message}, composite_key, logs)

            except Exception as e:
                error_message = str(e).replace("'", "''")
                logs.append(f"  - ERROR for record {composite_key}: {error_message}")
                try:
                    execute_update(session, "parameters", {"status": 'E', "message": error_message}, composite_key, logs)
                except Exception as update_e:
                    logs.append(f"  - CRITICAL: Failed to update error status for record. Error: {str(update_e)}")
                continue # Move to the next record

    except Exception as e:
        logs.append(f"A critical error occurred during the main SP execution: {str(e)}")

    logs.append("\nSP execution finished.")
    return "\n".join(logs)
