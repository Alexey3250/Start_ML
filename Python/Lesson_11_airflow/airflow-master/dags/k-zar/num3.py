from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def printi(task_number, **kwargs):
        return print(f"task number is: {task_number}")


with DAG(
        'sibir_3',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='sibir-task-3',
        schedule_interval=timedelta(days=3650),
        start_date=datetime(2022, 10, 20),
        catchup=False,
        tags=['sibir_3'],
) as dag:
    for i in range(10):
        m1 = BashOperator(
            task_id=f'bash_command{i}',
            bash_command=f"echo {i}")
    for task_number in range(20):
        m2 = PythonOperator(
            task_id=f'num_of_tasks_on_python{task_number}',
            op_kwargs={"task number": task_number},
            python_callable=printi
        )