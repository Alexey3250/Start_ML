from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


def First():
    return 'Airflow tracks everything'


def Second(ti):
    to_print = ti.xcom_pull(
        key='return_value',
        task_ids='first_task'
    )
    print(to_print)


with DAG(
        'HW_10_d-lobachev_XCom',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='DAG step 10 of HW Lesson 11',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 2, 10),
        catchup=False,
        tags=['lobachev'],
) as dag:
    t1 = PythonOperator(
        task_id='first_task',
        python_callable=First
    )

    t2 = PythonOperator(
        task_id='pull_from_xcom_task',
        python_callable=Second
    )

    t1 >> t2
