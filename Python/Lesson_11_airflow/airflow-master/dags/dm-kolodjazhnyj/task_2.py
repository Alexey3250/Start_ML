from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

def print_ds(ds):
    print(ds)

with DAG(
    'task_1_',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)},
    start_date=datetime(2022, 8, 9)
    ) as dag:

    t1 = BashOperator(
        task_id='print_pwd',  # id, будет отображаться в интерфейсе
        bash_command='pwd',  # какую bash команду выполнить в этом таске
    )

    t2 = PythonOperator(
        task_id='print_ds',
        python_callable=print_ds,
    )
    
    t1>>t2