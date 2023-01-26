from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def printi(ds):
    return print(ds)


with DAG(
    'norgello-1',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='first task in lesson №11',
    schedule_interval=timedelta(days=3650),
    start_date=datetime(2022, 10, 20),
    catchup=False
) as dag:
    m1=BashOperator(
        task_id='comm_pwd',
        bash_command='pwd',
)

    m2=PythonOperator(
        task_id='logical_date',
        python_callable=printi)
m1>>m2