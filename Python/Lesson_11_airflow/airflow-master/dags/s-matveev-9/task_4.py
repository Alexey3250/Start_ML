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


doc_string =  dedent(
            """\

            # заголовок

            Абзац

            `pi = 3.15`
            **полужирный**
            __полужирный__
            _курсив_
            *курсив*
            """
        )

def print_ds(task_number):
    print(f"task number is: {task_number}");


with DAG(
    "hw_4_s-matveev-9",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    for i in range(10):
        bash_task = BashOperator(
            task_id=f"hw4_msv_bash_task_{i}",
            bash_command=f"echo {i}",
        )
        bash_task.doc_md = doc_string
    for i in range(20):
        python_task = PythonOperator(
            task_id=f"hw4_msv_python_task_{i}",
            python_callable=print_ds,
            op_kwargs={"task_number": i}
        )
        python_task.doc_md = doc_string

    
