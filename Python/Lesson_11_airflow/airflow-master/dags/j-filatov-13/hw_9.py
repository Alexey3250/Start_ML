from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


def push_ti(ti):
    ti.xcom_push(
        key="sample_xcom_key",
        value="xcom test"
    )


def pull_ti(ti):
    v = ti.xcom_pull(
        key="sample_xcom_key",
        task_ids="push_ti"
    )
    print(v)


with DAG(
        'hw_9_j-filatov-13',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='This is a nineth exercise. Xcom.',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 10, 22),
        catchup=False,
        tags=['hw_9_j-filatov-13']
) as dag:

    push_operator = PythonOperator(
        task_id='push_ti',
        python_callable=push_ti,
    )

    pull_operator = PythonOperator(
        task_id='pull_ti',
        python_callable=pull_ti,
    )

    push_operator >> pull_operator
