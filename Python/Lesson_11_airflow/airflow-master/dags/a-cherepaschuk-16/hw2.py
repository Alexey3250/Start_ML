from datetime import timedelta, datetime

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

def date_print(ds):
    print(ds)

with DAG(
    'hw_1_cherepashchuk',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
description='hw1',
schedule_interval=timedelta(days=1),
start_date=datetime(2023, 1, 19),
catchup=False,
) as dag:
    task_1 = BashOperator(
        task_id = 'pwd',
        bash_command='pwd'
    )

    task_2 = PythonOperator(
        task_id = 'print_date',
        python_callable=date_print
    )

    task_1 >> task_2