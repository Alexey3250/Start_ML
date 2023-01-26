from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_n(task_number):
    print(f'task number is: {task_number}')

with DAG(
        'step_6_mishura',

        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },

        description='step_6',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 7, 20),
        catchup=False,
        tags=['step_6'],
) as dag:
    for i in range(30):
        if i < (10):
            task = BashOperator(
                task_id=f'task_{i}',
                env={'NUMBER': str(i)},
                bash_command='echo $NUMBER',
            )

        else:
            task = PythonOperator(
                task_id=f'print_{i}_task',
                python_callable=print_n,
                op_kwargs={'task_number' : i}
            )

        task