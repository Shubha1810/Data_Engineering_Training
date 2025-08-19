from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def say_hello():
    print("👋 Hello from Airflow in Docker!")

with DAG(
    dag_id="hello_world_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["demo"],
) as dag:
    task = PythonOperator(
        task_id="say_hello_task",
        python_callable=say_hello
    )
