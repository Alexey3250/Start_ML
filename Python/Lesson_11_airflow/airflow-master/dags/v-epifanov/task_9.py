from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from textwrap import dedent

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

def to_xcom(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test'
    )

def from_xcom(ti):
    sample_xcom_value = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='to_xcom'
    )
    print(sample_xcom_value)

def print_task_number(ts, run_id, **kwargs):
    print(f'task number is: {str(kwargs["task_number"])}')
    print(f'ts: {str(ts)}')
    print(f'run_id: {str(run_id)}')

with DAG(
    'task_9_vepifanov',
    description='vepifanov, задание 11.9',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['vepifanov'],
) as dag:

    t_1 = PythonOperator(
        task_id = f"to_xcom",
        python_callable=to_xcom)

    t_2 = PythonOperator(
        task_id = f"from_xcom",
        python_callable=from_xcom)

    t_1 >> t_2    
