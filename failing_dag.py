from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def failing_task():
    raise Exception("This task is designed to fail.")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 30),
    'retries': 0,
}

with DAG(
    dag_id='failing_dag',
    default_args=default_args,
    schedule_interval='@once',  # Run once for testing
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id='failing_task',
        python_callable=failing_task,
    )

task1
