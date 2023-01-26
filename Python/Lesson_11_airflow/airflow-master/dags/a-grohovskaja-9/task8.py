from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import json

def push_to_xcom(ti):
    return "Airflow tracks everything"

def pull_test(ti):
    sample_xcom_key = ti.xcom_pull(
        key='return_value',
        task_ids='push_test_string'
    )
    print(sample_xcom_key)

with DAG(
    'a-grohovskaja-9_hw_9',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
    description='First DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id = 'push_test_string',
        python_callable=push_to_xcom
    )
    t2 = PythonOperator(
        task_id = 'pull_test_string',
        python_callable=pull_test
    )

    t1 >> t2
