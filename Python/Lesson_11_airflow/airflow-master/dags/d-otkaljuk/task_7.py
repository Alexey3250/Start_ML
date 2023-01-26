from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os

def print_task_number(task_number):
    print(f"task number is: {task_number}")
    return "task number printed"


with DAG(
    # название
'hw_7_d-otkaljuk',
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
start_date=datetime(2022, 10, 31),
catchup=False,
tags=['hw_7_d_otkaljuk'],
) as dag:

    def task(task_number, ts, run_id):
        print(ts)
        print(run_id)
        print(f"task number is: {task_number}")

    for i in range(10):
        t_1 = PythonOperator(
            task_id=f"print_{i}",
            python_callable=task,
            op_kwargs={"task_number": i}
            # task_number - названием переменной def 'task' под
            # видом **kwargs, i - значение предаваемое в функцию
        )
    t_1