from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

from datetime import datetime, timedelta

def print_var():
    is_startml = Variable.get("is_startml")
    return print(is_startml)

with DAG(
    's-sehova-17-12',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),

    catchup=False,
    tags=['task12'],
) as dag:
    t1 = PythonOperator(
        task_id = 'print',
        python_callable = print_var,
        )
t1