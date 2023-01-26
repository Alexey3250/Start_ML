from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable


def read_variable():
    print(Variable.get("is_startml"))


with DAG(
    'a-kamentsev_dag12',
    # Параметры по умолчанию
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime.now(),
    catchup=False,
    tags=['a-kamentsev_dag12'],
) as dag:

    t1 = PythonOperator(
        task_id='print_variable',
        python_callable=read_variable
    )

    t1
