from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from datetime import datetime

@dag(start_date=datetime(2023, 1, 1), schedule_interval="*/2 * * * *", catchup=False)
def limit_dag():

    # Define tasks with resource requests and limits
    tasks = [
        BashOperator(
            task_id=f'task_{t}',
            bash_command='sleep 60',
            executor_config={
                "KubernetesExecutor": {
                    "request_cpu": "200m",
                    "limit_cpu": "400m",
                    "request_memory": "128Mi",
                    "limit_memory": "400Mi"
                }
            }
        ) for t in range(1, 4)
    ]

    @task
    def task_4(data):
        print(data)
        return 'done'
    
    @task
    def task_5(data):
        print(data)

    # Set task dependencies
    tasks >> task_5(task_4(42))

limit_dag()
