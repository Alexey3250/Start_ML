from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


def xcom_task_1(ti):
    ti.xcom_push(
            key="sample_xcom_key",
            value="xcom test"
    )


def xcom_task_2(ti):
    print(ti.xcom_pull(key="sample_xcom_key", task_ids='xcom_pusher'))



with DAG(
    'hw_9_a-astakhov',
    # Параметры по умолчанию
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime.now(),
    catchup=False,
    tags=['hw_9_a-astakhov'],
) as dag:

    t1 = PythonOperator(
        task_id='xcom_pusher',
        python_callable=xcom_task_1
    )


    t2 = PythonOperator(
        task_id='xcom_puller',
        python_callable=xcom_task_2
    )
    

    t1 >> t2
