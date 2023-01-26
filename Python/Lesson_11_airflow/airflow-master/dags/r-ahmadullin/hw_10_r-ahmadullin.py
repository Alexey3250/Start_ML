
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

import requests
import json


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
    'xcom_dag',
    start_date=datetime(2021, 1, 1),
    max_active_runs=2,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
    catchup=False
) as dag:
    def airflow_tracks():
        return "Airflow tracks everything"

    def pull_xcom_test(ti):
        xcom_test = ti.xcom_pull(
            key='return_value',
            task_ids='push_xcom'
        )
        print(xcom_test)

    t1 = PythonOperator(
        task_id='push_xcom',
        python_callable=airflow_tracks
    )
    t2 = PythonOperator(
        task_id='pull_xcom',
        python_callable=pull_xcom_test,
    )

    t1 >> t2
