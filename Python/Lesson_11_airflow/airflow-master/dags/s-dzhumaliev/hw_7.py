from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG

from airflow.operators.python import PythonOperator

def print_task_number(ts, run_id, task_number):
    print(ts, run_id, task_number)

with DAG(
    'HW_7_s-dzhumaliev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime(2022, 1, 1),
) as dag:
    for i in range(20):
        t = PythonOperator(
            task_id=f'print_task_number_{i}',
            op_kwargs={'task_number': i},
            python_callable=print_task_number
        )
