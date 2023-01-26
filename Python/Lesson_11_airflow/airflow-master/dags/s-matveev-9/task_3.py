from datetime import datetime, timedelta

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


def print_ds(task_number):
    print("task number is: {task_number}");


with DAG(
    "hw_3_s-matveev-9",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    for i in range(10):
        bash_task = BashOperator(
            task_id=f"hw3_msv_bash_task_{i}",
            bash_command=f"echo {i}",
        )
    for i in range(20):
        task2_python = PythonOperator(
            task_id=f"hw3_msv_python_task_{i}",
            python_callable=print_ds,
            op_kwargs={"task_number": i}
        )
