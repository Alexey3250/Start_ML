
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent
import os


with DAG(
        'o-gurylev_task_7',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='o-gurylev_task_7',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['o-gurylev_task_7']
) as dag:
    def task(task_number, ts, run_id):
        print(f"run_id is:-> {ts}")
        print(f"run_id is:-> {run_id}")
        print(f"task number is: {task_number}")
    for i in range(30):
        if i < 10:
            task_bash = BashOperator(
                task_id='bash_task_' + str(i),
                bash_command=f"echo {i}",
            )
        else:
            task_python = PythonOperator(
                task_id='python_task_' + str(i),
                python_callable=task,
                op_kwargs={'task_number': i},
            )
