from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_variable():
    from airflow.models import Variable
    is_startml = Variable.get("is_startml")
    print(is_startml)
    return is_startml

with DAG(
    'HW_12_a.platov',
    default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),         
        },
        description='Home Work N10 (XCom with key = return_value)',
        start_date=datetime(2022, 7, 24),
        catchup=False,
        tags=['a.platov'],
    ) as dag:
           
        t1 = PythonOperator(
            task_id='print_Variable',
            python_callable=print_variable,
            )
        t1
