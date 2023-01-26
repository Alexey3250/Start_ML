from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

import requests
import json

url = 'https://covidtracking.com/api/v1/states/'
state = 'wa'

def push_x_com_test(ti):
    ti.xcom_push(
        key='testing',
        value='xcom test'
    )

def get_x_com_test(ti):
    """
    Evaluates testing increase results
    """
    result = ti.xcom_pull(
        key='testing'
    )
    print(result)

# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'hw_9_e-tadevosjan',
    start_date=datetime(2021, 1, 1),
    max_active_runs=2,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
    catchup=False
) as dag:
    x_com_push = PythonOperator(
        python_callable=push_x_com_test
    )
    x_com_get = PythonOperator(
        python_callable=get_x_com_test
    )

    x_com_push >> x_com_get