from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def printi(**kwargs):
    return print(f"task number is: {kwargs.get('task_number')}")


with DAG(
        'norgello-2',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='first task in lesson №11',
        schedule_interval=timedelta(days=3650),
        start_date=datetime(2022, 10, 20),
        catchup=False
) as dag:
    for i in range(10):
        m1 = BashOperator(
            task_id=f'bash_command{i}',
            bash_command=f"echo {i}")
    for task_number in range(20):
        m2 = PythonOperator(
            task_id=f'number_tasks_on_python{task_number}',
            python_callable=printi,
            op_kwargs = {"task number": task_number}
        )