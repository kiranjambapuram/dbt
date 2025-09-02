CREATE OR REPLACE PROCEDURE trigger_dbt_job_sp()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python', 'requests', 'typing')
HANDLER = 'trigger_dbt_job'
EXTERNAL_ACCESS_INTEGRATIONS = (POC_EAI_TEST)
SECRETS = ('dbt_api_token' = POC_SECRET_TEST)
AS
$$
import snowflake.snowpark as snowpark
import requests
import json
from typing import List
from _snowflake import get_secret_string

def execute_update(session: snowpark.Session, table: str, updates: dict, conditions: dict) -> None:
    """Helper function to execute a parameterized UPDATE statement."""
    set_clause = ", ".join([f'"{k.upper()}" = ?' for k in updates.keys()])
    where_clause = " AND ".join([f'"{k.upper()}" = ?' for k in conditions.keys()])
    sql = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
    params = list(updates.values()) + list(conditions.values())
    session.sql(sql, params=params).collect()

def get_environment(session: snowpark.Session) -> str:
    """Determines the environment ('dev', 'acc', 'prod') from the current database name."""
    db_name = session.sql("SELECT current_database()").collect()[0][0].upper()
    if 'DEV' in db_name:
        return 'dev'
    elif 'ACC' in db_name:
        return 'acc'
    elif 'PROD' in db_name:
        return 'prod'
    else:
        raise ValueError(f"Could not determine environment from database name '{db_name}'.")

def trigger_dbt_job(session: snowpark.Session) -> str:
    """
    The main handler function that triggers dbt Cloud jobs dynamically and logs to the table.
    """
    sp_logs = ["SP execution started: Dynamic Batch mode."]

    try:
        environment = get_environment(session)
        sp_logs.append(f"Determined environment: '{environment}'.")

        records_to_process_df = session.table("parameters").filter(snowpark.functions.col("status") == 'N').order_by("timestamp")
        records = records_to_process_df.collect()

        if not records:
            sp_logs.append("No new records to process. Exiting.")
            return "\n".join(sp_logs)

        sp_logs.append(f"Found {len(records)} records to process.")

        dbt_api_token = get_secret_string('dbt_api_token')
        dbt_account_id = "YOUR_DBT_ACCOUNT_ID"
        headers = { "Authorization": f"Token {dbt_api_token}", "Content-Type": "application/json" }

        for i, record_row in enumerate(records):
            record_logs = []
            record = record_row.as_dict()
            composite_key = {"period": record["PERIOD"], "org_code": record["ORG_CODE"], "segment": record["SEGMENT"], "region": record["REGION"], "user": record["USER"], "timestamp": record["TIMESTAMP"]}
            sp_logs.append(f"\nProcessing record {i+1}/{len(records)}: {composite_key}")

            try:
                segment = record["SEGMENT"]
                region = record["REGION"]
                record_logs.append(f"Looking up job config for env='{environment}', segment='{segment}', region='{region}'...")
                config_df = session.table("dbt_job_config").filter(
                    (snowpark.functions.col("ENVIRONMENT") == environment) &
                    (snowpark.functions.col("SEGMENT") == segment) &
                    (snowpark.functions.col("REGION") == region)
                )
                job_config = config_df.collect()

                if not job_config:
                    raise ValueError(f"No entry found in dbt_job_config for this combination.")

                dbt_job_id = job_config[0]["DBT_JOB_ID"]
                start_dbt_model = job_config[0]["START_DBT_MODEL"]
                record_logs.append(f"Found config: dbt_job_id={dbt_job_id}, start_dbt_model='{start_dbt_model}'")

                execute_update(session, "parameters", {"status": 'P'}, composite_key)
                record_logs.append("Locked record with status 'P'.")

                api_url = f"https://cloud.getdbt.com/api/v2/accounts/{dbt_account_id}/jobs/{dbt_job_id}/run/"
                dbt_vars = {"reporting_period": record["PERIOD"], "ps_org_code": record["ORG_CODE"], "ps_segment": segment, "ps_region": region}
                vars_json_string = json.dumps(dbt_vars)
                steps_override_command = f"dbt run -s {start_dbt_model} --vars '{vars_json_string}'"

                payload = {"cause": f"Triggered by Snowflake SP for period {record['PERIOD']}", "schema_override": f"dbt_cloud_{record['ORG_CODE']}_{segment}_{region}", "steps_override": [steps_override_command]}
                record_logs.append(f"Calling API at {api_url} with payload: {json.dumps(payload)}")

                response = requests.post(api_url, headers=headers, data=json.dumps(payload))
                response.raise_for_status()

                response_data = response.json()
                run_id = response_data.get("data", {}).get("id")

                if run_id is None:
                    raise Exception("Could not find 'id' in dbt API response.")

                success_message = f"API call successful. dbt run initiated with run_id: {run_id}."
                record_logs.append(success_message)

                update_payload = {"status": 'S', "dbt_run_id": run_id, "message": success_message, "sp_logs_trigger": "\n".join(record_logs)}
                execute_update(session, "parameters", update_payload, composite_key)

            except Exception as e:
                error_message = str(e).replace("'", "''")
                record_logs.append(f"ERROR: {error_message}")
                try:
                    update_payload = {"status": 'E', "message": error_message, "sp_logs_trigger": "\n".join(record_logs)}
                    execute_update(session, "parameters", update_payload, composite_key)
                except Exception as update_e:
                    sp_logs.append(f"CRITICAL: Failed to update error status for record {composite_key}. Error: {str(update_e)}")
                continue

    except Exception as e:
        sp_logs.append(f"A critical error occurred during the main SP execution: {str(e)}")

    sp_logs.append("\nSP execution finished.")
    return "\n".join(sp_logs)
$$;
