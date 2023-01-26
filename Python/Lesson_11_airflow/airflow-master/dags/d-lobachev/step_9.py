from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


def Push_to_xcom(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test'
    )


def Pull_from_xcom(ti):
    to_print=ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='push_to_xcom_task'
    )
    print(to_print)


with DAG(
        'HW_9_d-lobachev_KWARGS',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='DAG step 9 of HW Lesson 11',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 2, 10),
        catchup=False,
        tags=['lobachev'],
) as dag:
    t1 = PythonOperator(
        task_id = 'push_to_xcom_task',
        python_callable=Push_to_xcom
    )

    t2 = PythonOperator(
        task_id = 'pull_from_xcom_task',
        python_callable=Pull_from_xcom
    )

    t1>>t2


