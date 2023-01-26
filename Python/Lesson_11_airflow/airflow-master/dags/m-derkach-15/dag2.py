"""
Task 3
"""

from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_task_num(task_number, **kwargs):
    print(f'task number is: {task_number}')


with DAG(
        'derkach_dag2',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
        description='Dag for task_2',
        schedule_interval=timedelta(minutes=5),
        start_date=datetime(2022, 12, 22)
) as dag:
    bash_tasks = []

    for i in range(10):
        task = BashOperator(
            task_id=f'task_bash_{i}',
            bash_command=f'echo {i}'
        )
        bash_tasks.append(task)

    python_tasks = []

    for j in range(20):
        task = PythonOperator(
            task_id=f'task_python_{j}',
            python_callable=print_task_num,
            op_kwargs={'task_number': j}
        )

        python_tasks.append(task)

    bash_tasks >> python_tasks
