from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta

with DAG(
    'hw_2_p-elagin-9',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='hw_2_p-elagin-9',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['p-elagin-9'],

) as dag:
    bash_task = BashOperator(
        task_id='pwd',
        bash_command='pwd',
    )
    
    def print_ds(ds, **kwarg):
        print(ds)
        return 'Любое другое сообщение'

    py_task = PythonOperator(
        task_id='print_ds',
        python_callable=print_ds,
    )

    bash_task >> py_task
