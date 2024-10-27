from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from datetime import datetime

@dag(start_date=datetime(2023, 1, 1), schedule='@daily', catchup=False)
def etl_dag():

    # Task 1: Extract data
    extract_task = BashOperator(
        task_id='extract_data',
        bash_command='echo "Extracting data..." && sleep 5'
    )

    # Task 2: Transform data
    @task
    def transform_data():
        # This could include data transformation logic
        print("Transforming data...")
        transformed_data = "Transformed Data"
        return transformed_data

    # Task 3: Load data
    @task
    def load_data(transformed_data):
        # This would include logic to load data into a target location
        print(f"Loading data: {transformed_data}")

    # Setting up the task dependencies
    transformed_data = transform_data()
    extract_task >> transformed_data >> load_data(transformed_data)

etl_dag = etl_dag()
