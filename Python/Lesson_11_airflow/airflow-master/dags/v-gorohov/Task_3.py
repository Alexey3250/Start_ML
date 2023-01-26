from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args={
    'start_date': datetime(2022, 11, 18),
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

def print_task_number(task_number):
    print(f"task number is: {task_number}")

with DAG(
    "hw_3_v-gorohov_cool_dag",
    default_args=default_args,
    schedule_interval=None
) as dag:

    for i in range(10):
        bash_operator = BashOperator(
            task_id=f"bash_{i}",
            bash_command=f"echo {i*i}",
            dag=dag
        )

    for i in range(20):
        python_operator = PythonOperator(
            task_id=f"python_{i}",
            python_callable=print_task_number,
            op_kwargs={
                "task_number": i
            },
            dag=dag
        )