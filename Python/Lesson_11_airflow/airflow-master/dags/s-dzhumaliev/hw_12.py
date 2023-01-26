from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.models import Variable


def print_var():
    is_startml = Variable.get("is_startml")
    print(is_startml)

with DAG(
    'HW_12_s-dzhumaliev',
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
    PythonOperator(
        task_id='print_var',
        python_callable=print_var
    )