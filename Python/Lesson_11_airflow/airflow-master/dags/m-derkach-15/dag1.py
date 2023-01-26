"""
Task 2 AirFlow
"""
from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_ds(ds, **kwargs):
    print(ds)


with DAG(
        'derkach_dag1',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='dag for task_1',
        schedule_interval=timedelta(minutes=5),
        start_date=datetime(2022, 12, 20)
) as dag:
    t1 = BashOperator(
        task_id='print_current_directory',
        bash_command='pwd'
    )

    t2 = PythonOperator(
        task_id='print_id',
        python_callable=print_ds
    )

    t1 >> t2