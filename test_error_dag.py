from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'error_pod_dag',
    default_args=default_args,
    description='A DAG to intentionally create an error in the Kubernetes pod',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Task that will fail and cause the pod to go into an Error state
    fail_task = KubernetesPodOperator(
        namespace='default',  # Replace with your namespace if different
        image='busybox',  # Using busybox as it’s a small image with shell support
        cmds=['sh', '-c'],
        arguments=['exit 1'],  # `exit 1` will cause a failure
        name='intentional-fail-pod',
        task_id='intentional_fail_task',
        get_logs=True,
        is_delete_operator_pod=True,
    )

    fail_task
