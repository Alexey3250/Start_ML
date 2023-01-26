from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta


def print_loc_date(**kwargs):
    print(kwargs['ds'])


with DAG(
    'step_2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='DAG for step_2',
    shedule_interval=timedelta(days=1),
    start_date=datetime(2022, 11, 12),
    catchup=False,
    tags=['step_2']
) as dag:
    t1 = BashOperator(
        task_id='print current dir',
        bash_command='pwd'
    )
    t2 = PythonOperator(
        task_id='print local date',
        python_collable=print_loc_date
    )
t1 >> t2
