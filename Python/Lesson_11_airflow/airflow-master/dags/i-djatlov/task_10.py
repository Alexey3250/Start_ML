from textwrap import dedent
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_10_i-djatlov',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description='task_10',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 11, 1),
    catchup=False,
    tags=['i-djatlov']
) as dag:
    def print_string():
        return('Airflow tracks everything')
    
    def func_pull(ti):
        pull_func = ti.xcom_pull(
            key='return_value',
            task_ids='print_string'
        )
        print(pull_func)
        
        
    t1 = PythonOperator(
        task_id='print_string',
        python_callable=print_string,
    )
    
    t2 = PythonOperator(
        task_id='retval',
        python_callable=func_pull,
    )
    
    t1 >> t2
            

