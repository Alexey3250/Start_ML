from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def get_testing_increase(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value="xcom test"
    )


def analyze_testing_increases(ti):
    push_text = ti.xcom_pull(
        key='sample_xcom_key',
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
        python_callable=get_testing_increase,
    )

    push_data = PythonOperator(
        task_id='push_',
        python_callable=analyze_testing_increases,
    )

    get_data >> push_data
