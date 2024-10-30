from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define a function that consumes a lot of memory
def memory_hog():
    # Attempt to allocate a large list to trigger OOM
    large_list = []
    while True:
        large_list.append(' ' * (10**6))  # Allocate 1 MB at a time

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 30),
    'retries': 0,
}

with DAG(
    dag_id='oom_dag',
    default_args=default_args,
    schedule_interval='@once',  # Run once for testing
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id='memory_hog_task',
        python_callable=memory_hog,
    )

task1
