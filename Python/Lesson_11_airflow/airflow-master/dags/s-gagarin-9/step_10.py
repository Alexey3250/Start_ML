from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def string_to_xcom():
    return f"Airflow tracks everything"


def get_string_from_xcom(ti):
    res = ti.xcom_pull(
        key='return_value',
        task_ids="push_string"
    )
    print(res)


with DAG(
    'step_10_gagarin',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='DAG_for_step_10',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 11, 12),
    catchup=False,
    tags=['step_10_gagarin']
) as dag:
    task_1 = PythonOperator(
        task_id='push_string',
        python_callable=string_to_xcom
    )
    task_2 = PythonOperator(
        task_id='get_string',
        python_callable=get_string_from_xcom
    )
task_1 >> task_2
