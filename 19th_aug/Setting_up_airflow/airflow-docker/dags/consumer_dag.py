from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


# Python function to read the file
def read_file():
    try:
        with open('/tmp/data.txt', 'r') as f:
            content = f.read()
            print("✅ Consumer DAG read this content:", content)
    except FileNotFoundError:
        print("⚠️ File not found! Make sure producer_dag.py has created it first.")


# Define the DAG
with DAG(
    dag_id='consumer_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['example']
) as dag:

    read_task = PythonOperator(
        task_id='read_file_task',
        python_callable=read_file
    )
