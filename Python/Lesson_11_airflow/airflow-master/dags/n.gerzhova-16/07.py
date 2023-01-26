from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import requests
import json

def push_test_value(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test'
    )

def pull_test_value(ti):
    value = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='xcom_test_value'
    )
    print(value)

with DAG(
    'NG_seventh',
    default_args={
        'depends_on_past': False,
        'email': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },

    description='DAG for Task 07',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 7, 23),
) as dag:
    
    t1 = PythonOperator(
        task_id = 'xcom_test_value',
        python_callable=push_test_value,
        )

    t2 = PythonOperator(
        task_id = 'xcom_test_value_pull',
        python_callable= pull_test_value,
        )

    t1 >> t2
