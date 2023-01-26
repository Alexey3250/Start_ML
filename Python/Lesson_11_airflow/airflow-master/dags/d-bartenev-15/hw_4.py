"""
Test documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_task(task_number):
    print(f'task number is: {task_number}')


with DAG(
        'hw_4',
        default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)},
        description='A simple tutorial DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['example'],
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id=f'echo_{i}',
            bash_command=f'echo {i}'
        )
        t1.doc_md = dedent(
            '''\
            # A Task
            This is `code`
            *BOLD* **CURSIVE**
            ''')

    for i in range(20):
        t2 = PythonOperator(
            task_id=f'print_task{i}',
            python_callable=print_task,
            op_kwargs={'task_namber': i + 10}
        )
        t2.doc_md = dedent(
            '''\
            # A Task
            This is `code`
            *BOLD* **CURSIVE**
            ''')