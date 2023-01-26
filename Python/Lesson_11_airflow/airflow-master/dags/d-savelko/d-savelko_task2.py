from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'd-savelko_task2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    for i in range(10):
        task = BashOperator(
            task_id=f'task_bash_{i}',
            bash_command=dedent(f"echo {i}")
        )
        task >> task

    def task_number_is(task_number):
        return f"task number is: {task_number}"
    
    for task_number in range(20):
        task = PythonOperator(
            task_id=f'python_oper_{task_number}',
            python_callable=task_number_is,
            op_kwargs={'task_number':task_number}
        )
        task >> task