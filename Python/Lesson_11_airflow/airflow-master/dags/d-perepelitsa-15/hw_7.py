from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

def print_world(task_number: str, ts: str, run_id: str):
    print(f"task number is: {task_number}")
    print(ts, run_id)

with DAG(
        dag_id='dag_hw_6_perepelitsa',
        default_args=default_args,
        description='Задание hw_6',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 1, 11),
        catchup=False
) as dag:
    for i in range(20):
        task2 = PythonOperator(
            task_id='task_python' + str(i),
            python_callable=print_world,
            op_kwargs={'task_number': i}
        )