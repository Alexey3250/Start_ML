from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def get_test(ti):
    ti.xcom_push(key='return_value')
    return "Airflow tracks everything"


def push_test(ti):
    push_text = ti.xcom_pull(
        key='return_value',
        task_ids='get_'
    )
    return push_text


with DAG(
    'hw_8_o-murashova-13',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 11, 11),
    tags=['example'],
) as dag:

    get_data = PythonOperator(
        task_id='get_',
        python_callable=get_test,
    )

    push_data = PythonOperator(
        task_id='push_',
        python_callable=push_test,
    )

    get_data >> push_data
