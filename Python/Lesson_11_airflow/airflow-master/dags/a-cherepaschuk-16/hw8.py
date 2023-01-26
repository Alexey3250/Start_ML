from datetime import timedelta, datetime

from airflow import DAG

from airflow.operators.python_operator import PythonOperator

def push_xcom(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test'
    )
def pull_xcom(ti):
    pulled = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='cherepashchuk_xcom_push'
    )

with DAG(
    'cherepashchuk_xcom',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
description='hw1',
schedule_interval=timedelta(days=1),
start_date=datetime(2023, 1, 19),
catchup=False,
) as dag:
    task_1 = PythonOperator(
        task_id = 'cherepashchuk_xcom_push',
        python_callable=push_xcom
    )

    task_2 = PythonOperator(
        task_id = 'cherepashchuk_xcom_pull',
        python_callable=pull_xcom
    )

    task_1 >> task_2