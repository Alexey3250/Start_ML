from datetime import timedelta, datetime
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

def task_num(task_number):
    print(f"task number is: {task_number}")

with DAG(
    'hw_1_cherepashchuk',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
description='hw1',
schedule_interval=timedelta(days=1),
start_date=datetime(2023, 1, 19),
catchup=False,
) as dag:
    def task_num(task_number):
        print(f"task number is: {task_number}")

    for i in range(30):
        if i < 10:
            task = BashOperator(
                task_id="task_number_"+str(i),
                bash_command=f"echo {i}",
            )
        else:
            task = PythonOperator(
                task_id="task_number_"+str(i),
                python_callable=task_num,
                op_kwargs={'task_number' : int(i)},
            )
    task.doc_md = dedent(
        """
            # DAG DOCUMENTATION
            You can **document** your _task_ using the attributes `doc_md` (markdown),
            `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
            rendered in the UI's Task Instance Details page
            `def task_num(task_number):
                print(f"task number is: {task_number}")`
        """
    )

