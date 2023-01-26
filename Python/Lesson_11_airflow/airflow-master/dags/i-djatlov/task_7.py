from textwrap import dedent
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_7_i-djatlov',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description='task_7',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 11, 1),
    catchup=False,
    tags=['example']
) as dag:
    def func(ts, run_id, task_number):
        print(ts)
        print(run_id)
        print(task_number)
    for i in range(30):
        t1 = PythonOperator(
            task_id=f'task_number_{i}',
            python_callable=func,
            op_kwargs={'task_number': i},
        )
            

