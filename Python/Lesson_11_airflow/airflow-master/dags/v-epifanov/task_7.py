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

def print_task_number(ts, run_id, **kwargs):
    print(f'task number is: {str(kwargs["task_number"])}')
    print(f'ts: {str(ts)}')
    print(f'run_id: {str(run_id)}')

with DAG(
    'task_7_vepifanov',
    description='vepifanov, задание 11.7',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['vepifanov'],
) as dag:
    for i in range(10):
        task = PythonOperator(
            task_id = f'python_task_{i}',
            python_callable=print_task_number,
            op_kwargs = {'task_number': i}
        )
        task
        
