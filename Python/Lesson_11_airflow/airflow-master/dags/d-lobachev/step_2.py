from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'HW_2_d-lobachev_First_DAG',
    default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'First DAG from step 2 of HW Lesson 11',
    schedule_interval = timedelta(days=1),
    start_date=datetime(2022, 2, 10),
    catchup = False,
    tags = ['first'],
) as dag:

    t1 = BashOperator(
        task_id = 'print_dir',
        bash_command = 'pwd',
    )

    def print_ds(ds):
        """Печать ds"""
        print('Logical date is: ')
        print(ds)

    t2 = PythonOperator(
        task_id = 'print_ds',
        python_callable = print_ds,
    )

    t1 >> t2