from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import requests
import json

with DAG(
        'a-kamentsev_dag10',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 11, 25),
        catchup=False,
        tags=['a-kamentsev_dag10']
) as dag:
    def push_test():
        return 'Airflow tracks everything'


    t1 = PythonOperator(
        task_id='push_test',
        python_callable=push_test,
    )


    def pull_test(ti):
        print(
            ti.xcom_pull(
                key='return_value',
                task_ids='push_test'
            ))


    t2 = PythonOperator(
        task_id='pull_test',
        python_callable=pull_test,
    )

    t1 >> t2