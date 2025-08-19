from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import os

# File path for intermediary storage
EXTRACTED_FILE = "/tmp/extracted_data.txt"
TRANSFORMED_FILE = "/tmp/transformed_data.txt"

# Python function to simulate extraction
def extract_data():
    message = "raw_data: airflow,python,bash,etl"
    with open(EXTRACTED_FILE, "w") as f:
        f.write(message)
    print(f"[Extract Task] Data written to {EXTRACTED_FILE}")

# Python function to simulate transformation
def transform_data():
    with open(EXTRACTED_FILE, "r") as f:
        raw = f.read()
    transformed = raw.upper().replace("RAW_DATA:", "TRANSFORMED_DATA:")
    with open(TRANSFORMED_FILE, "w") as f:
        f.write(transformed)
    print(f"[Transform Task] Data transformed and stored in {TRANSFORMED_FILE}")

# DAG definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1
}

with DAG(
    dag_id="etl_pipeline_dag",
    default_args=default_args,
    start_date=datetime(2025, 8, 1),
    schedule_interval=None,   # Manual trigger
    catchup=False,
    tags=["ETL", "assignment"]
) as dag:

    # Step 1: Extract
    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract_data
    )

    # Step 2: Transform
    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform_data
    )

    # Step 3: Load (using BashOperator to simulate load)
    load_task = BashOperator(
        task_id="load",
        bash_command=f"cat {TRANSFORMED_FILE} && echo '[Load Task] Data loading complete!'"
    )

    # Task chaining
    extract_task >> transform_task >> load_task
