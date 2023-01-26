from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'hw_2_o-murashova-13',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),},
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 11, 11),
    tags=['example'],
) as dag:
    def print_(ds, **kwargs):
        print(kwargs)
        print(ds)
        return 'I have no idea how it works'


    t1 = BashOperator(
        task_id='first_dws_run',
        bash_command='pwd',)
    t2 = PythonOperator(
        task_id='first_ds_print',
        python_callable=print_,
    )

t1 >> t2
