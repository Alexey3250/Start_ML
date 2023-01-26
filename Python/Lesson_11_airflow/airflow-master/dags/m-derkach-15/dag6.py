"""
Task 3
"""

from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_task_num(task_number, ts, run_id, **kwargs):
    print(f'task number is: {task_number}')
    print(ts)
    print(run_id)


with DAG(
        'derkach_dag7',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
        description='Dag for task_7',
        schedule_interval=timedelta(minutes=5),
        start_date=datetime(2022, 12, 22)
) as dag:
    for j in range(20):
        task = PythonOperator(
            task_id=f'task_python_{j}',
            python_callable=print_task_num,
            op_kwargs={'task_number': j}
        )
