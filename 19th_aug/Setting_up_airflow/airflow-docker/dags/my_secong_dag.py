from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


# Functions for tasks
def task_one():
    print("Task One is running")


def task_two():
    print("Task Two is running after Task One")


# Define the DAG
with DAG(
        dag_id="my_second_dag",
        start_date=datetime(2025, 8, 18),
        schedule_interval="@daily",  # runs once every day
        catchup=False,
) as dag:
    # Task 1
    t1 = PythonOperator(
        task_id="task_one",
        python_callable=task_one
    )

    # Task 2
    t2 = PythonOperator(
        task_id="task_two",
        python_callable=task_two
    )

    # Set order of execution
    t1 >> t2  # means t1 runs first,
