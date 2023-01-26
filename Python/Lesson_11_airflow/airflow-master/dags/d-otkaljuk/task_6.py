from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_task_number(task_number):
    print(f"task number is: {task_number}")
    return "task number printed"


with DAG(
    # название
'hw_6_d-otkaljuk',
default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
description='Py and Bush operation',
schedule_interval=timedelta(days=1),
start_date=datetime(2022, 10, 29),
catchup=False,
tags=['hw_6_d_otkaljuk'],
) as dag:
    for i in range(10):
        t_1 = BashOperator(
            task_id=f"echo_task_number{i}",
            bash_command="echo '$NUMBER'",
            env = {"NUMBER": i}
        )

    t_1