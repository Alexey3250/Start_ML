
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent
import os

with DAG(
        'o-gurylev_task_6',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='o-gurylev_task_6',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['o-gurylev_task_6']
) as dag:
    for i in range(10):
        task = BashOperator(
            task_id='bashop' + str(i),
            bash_command=f'echo $NUMBER',
            env={'NUMBER': str(i)},
        )

