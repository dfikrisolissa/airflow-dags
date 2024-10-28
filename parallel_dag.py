from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime

@dag(start_date=datetime(2023, 1, 1), schedule='@daily', catchup=False)
def parallel_dag():

    # Task list using KubernetesPodOperator with resource requests and limits
    tasks = [
        KubernetesPodOperator(
            task_id=f'task_{t}',
            name=f'pod_task_{t}',
            namespace='default',
            image='alpine:3.14',  # Use a simple lightweight image for demo
            cmds=["sh", "-c", "sleep 60"],
            resources={
                'request_memory': '256Mi',
                'request_cpu': '250m',
                'limit_memory': '512Mi',
                'limit_cpu': '500m'
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

    tasks >> task_5(task_4(42))

parallel_dag = parallel_dag()


# from airflow.decorators import dag, task
# from airflow.operators.bash import BashOperator

# from datetime import datetime

# @dag(start_date=datetime(2023, 1 , 1), schedule='@daily', catchup=False)
# def parallel_dag():

#     tasks = [BashOperator(task_id='task_{0}'.format(t), bash_command='sleep 60'.format(t)) for t in range(1, 4)]

#     @task
#     def task_4(data):
#         print(data)
#         return 'done'
    
#     @task
#     def task_5(data):
#         print(data)

#     tasks >> task_5(task_4(42))

# parallel_dag()
