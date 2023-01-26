from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent


def print_task(task_number):
    print(f"task number is: {task_number}")

with DAG(

    'task2_m-vasilevskij',

    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    start_date = datetime(2022, 12, 14),
) as dag:

    for i in range(1, 31):
        if i < 10:
            task = BashOperator(
                task_id = f"task_{i}",
                bash_command = "echo $NUMBER",
                env = {'NUMBER': i}
            )
        else:
            task1 = PythonOperator(
                task_id = f"task_{i}",
                python_callable = print_task,
                op_kwargs = {'task_number': i}
            )



    task >> task1





