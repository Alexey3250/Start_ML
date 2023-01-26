from datetime import datetime, timedelta
from textwrap import dedent
from airflow.models import Variable

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook

with DAG(
    'hw_12_p-tarabanov',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    
    description='12 task',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 9, 11),
    catchup=False,
    tags=['Variable is_startml'],
) as dag:
    
    def get_variable():
        is_prod = Variable.get("is_startml")
        print(is_prod)
        return is_prod
    
    task1 = PythonOperator(
        task_id = 'task_get_variable',
        python_callable = get_variable 
    )
    
    task1