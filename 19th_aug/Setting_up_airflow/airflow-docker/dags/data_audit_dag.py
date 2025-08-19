from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import json
import random
import os

# File for storing audit results
AUDIT_FILE = "/tmp/audit_result.json"

# -------------------------------
# Task Functions
# -------------------------------

def pull_data():
    """
    Simulate pulling data from external API/DB.
    """
    # Simulated external data (random value)
    data = {"timestamp": datetime.utcnow().isoformat(), "value": random.randint(1, 100)}
    print(f"[Pull Task] Pulled data: {data}")
    return data


def validate_rule(**context):
    """
    Validate business rule: value must be <= 70.
    """
    data = context['ti'].xcom_pull(task_ids='data_pull')
    if not data:
        raise ValueError("No data pulled for validation.")

    # Business rule check
    if data["value"] > 70:
        result = {"status": "FAIL", "reason": f"Value {data['value']} exceeds threshold"}
        with open(AUDIT_FILE, "w") as f:
            json.dump(result, f)
        print("[Validation Task] Validation failed.")
        raise ValueError(result["reason"])
    else:
        result = {"status": "PASS", "data": data}
        with open(AUDIT_FILE, "w") as f:
            json.dump(result, f)
        print("[Validation Task] Validation passed.")
    return result


def log_audit():
    """
    Log audit results from file.
    """
    if not os.path.exists(AUDIT_FILE):
        raise FileNotFoundError("Audit result file not found.")

    with open(AUDIT_FILE, "r") as f:
        result = json.load(f)
    print(f"[Log Task] Audit result: {result}")
    return result


# -------------------------------
# DAG Definition
# -------------------------------

default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "email": ["alerts@datateam.com"],
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="data_audit_dag",
    default_args=default_args,
    description="Event-driven style DAG for data auditing",
    schedule_interval="@hourly",
    start_date=datetime(2025, 8, 1),
    catchup=False,
    tags=["audit", "data-quality"],
) as dag:

    # Task 1: Data Pull
    data_pull = PythonOperator(
        task_id="data_pull",
        python_callable=pull_data
    )

    # Task 2: Audit Validation
    audit_validation = PythonOperator(
        task_id="audit_validation",
        python_callable=validate_rule,
        provide_context=True
    )

    # Task 3: Logging audit result
    log_results = PythonOperator(
        task_id="log_results",
        python_callable=log_audit
    )

    # Task 4: Final Status (BashOperator)
    final_status = BashOperator(
        task_id="final_status",
        bash_command="echo '[Final Task] Data Audit DAG completed successfully!'"
    )

    # Chaining tasks
    data_pull >> audit_validation >> log_results >> final_status
