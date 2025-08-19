from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


# Python function to write a file
def write_file():
    with open('/tmp/data.txt', 'w') as f:
        f.write("This is the dataset written by Producer DAG.")


# Define the DAG
with DAG(
    dag_id='producer_dag',
    start_date=datetime(2023, 1, 1),  # ✅ Correct syntax
    schedule_interval=None,
    catchup=False,
    tags=['example']
) as dag:

    write_task = PythonOperator(
        task_id='write_file_task',
        python_callable=write_file
    )
