from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_ds(ds, **kwargs):
    print(ds)

with DAG(
    'hw2',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='hw2 DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    
    t2 = PythonOperator(
        task_id='print_pdate',
        python_callable=print_ds,
    )
    t1 = BashOperator(
        task_id='print_pwd',  
        bash_command='pwd',
    )
    
    t1 >> t2