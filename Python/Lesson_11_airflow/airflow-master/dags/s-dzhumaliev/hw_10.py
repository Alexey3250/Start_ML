from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG

from airflow.operators.python import PythonOperator

def set_to_xcom():
    return "Airflow tracks everything"

def read_from_xcom(ti):
    value = ti.xcom_pull(
        key='return_value',
        task_ids='set_to_xcom'
    )
    print(value)

with DAG(
    'HW_10_s-dzhumaliev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime(2022, 1, 1),
) as dag:
    t1 = PythonOperator(
        task_id='set_to_xcom',
        python_callable=set_to_xcom
    )

    t2 = PythonOperator(
        task_id='read_from_xcom',
        python_callable=read_from_xcom
    )

    t1 >> t2
