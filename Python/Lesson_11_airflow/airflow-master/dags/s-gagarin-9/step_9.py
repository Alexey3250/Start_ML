from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def push_value(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value="xcom test"
    )


def pull_print_data(ti):
    res = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids="data_into_XCom"
    )
    print(res)


with DAG(
    'step_9_gagarin',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='DAG_for_step_9',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 11, 12),
    catchup=False,
    tags=['step_9_gagarin']
) as dag:
    task_push = PythonOperator(
        task_id="data_into_XCom",
        python_callable=push_value
    )
    task_pull = PythonOperator(
        task_id='print_data_from_XCom',
        python_callable=pull_print_data

    )
task_push >> task_pull
