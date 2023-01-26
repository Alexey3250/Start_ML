from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

def print_world(ds: str):
    print(ds)

templated_command = dedent(
    """pwd"""
)

with DAG(
        dag_id='dag_hw_2',
        default_args=default_args,
        description='Задание hw_2',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 1, 11),
        catchup=False
) as dag:
    task1 = PythonOperator(
        task_id='task_1',
        python_callable=print_world
    )
    task2 = BashOperator(
        task_id='task_2',
        bash_command=templated_command
    )
    task2 >> task1