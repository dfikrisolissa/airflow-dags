from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import V1ResourceRequirements
from datetime import datetime

@dag(start_date=datetime(2023, 1, 1), schedule='@daily', catchup=False)
def parallel_dag():

    # Define resource requests and limits
    resource_requirements = V1ResourceRequirements(
        requests={"memory": "256Mi", "cpu": "250m"},
        limits={"memory": "512Mi", "cpu": "500m"}
    )

    # Task list using KubernetesPodOperator with corrected resource requests and limits
    tasks = [
        KubernetesPodOperator(
            task_id=f'task_{t}',
            name=f'pod_task_{t}',
            namespace='default',
            image='alpine:3.14',  # Use a simple lightweight image for demo
            cmds=["sh", "-c", "sleep 60"],
            resources=resource_requirements
        ) for t in range(1, 4)
    ]

    @task
    def task_4(data):
        print(data)
        return 'done'
    
    @task
    def task_5(data):
        print(data)

    tasks >> task_5(task_4(42))

parallel_dag = parallel_dag()
