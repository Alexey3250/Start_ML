from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'Lesson_11_step_6',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='My second training DAG',
        start_date=datetime(2022, 6, 15),
        schedule_interval=timedelta(days=1),
        catchup=False,
        tags=['e.zabuzov', 'step_6']
) as dag:
    def print_task_number(task_number, **kwargs):
        print(f'task number is: {task_number}')


    for i in range(10):
        NUMBER = i
        t_bash = BashOperator(
            task_id=f'BashOperator_{i}',
            bash_command=f"echo $NUMBER",
            env={'NUMBER': str(i)}
        )

