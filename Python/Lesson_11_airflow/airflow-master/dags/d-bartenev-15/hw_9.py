"""
Test documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def push_value(ti):
    ti.xcom_push(key='sample_xcom_key', value='xcom test')

def pull_value(ti):
    ti.xcom_pull(key='sample_xcom_key', task_ids='push_values')


def print_task(ts, run_id, task_number):
    print(f'task number is: {task_number}')
    print(f'ts: {ts}')
    print(f'run_id: {run_id}')

with DAG(
        'hw_9_bartenev',
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
    t1 = PythonOperator(
        task_id='push_values',
        python_callable=push_value
    )

    t2 = PythonOperator(
        task_id='pull_values',
        python_callable=pull_value
    )

    t1 >> t2