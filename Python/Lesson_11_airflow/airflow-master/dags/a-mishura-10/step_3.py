from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_n(task_number):
    print(f'task number is: {task_number}')

with DAG(
        'step_2_mishura',

        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },

        description='step_2',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 7, 20),
        catchup=False,
        tags=['step_2'],
) as dag:
    for i in range(30):
        if i < (10):
            task = BashOperator(
                task_id=f'create_{i}_file',
                bash_command=f'echo "ahahaha" > {i}.txt'
            )
            task.doc_md = """
                task creates txt file for each iteration
                with iteration num as filename
            """

        else:
            task = PythonOperator(
                task_id=f'print_{i}_task',
                python_callable=print_n,
                op_kwargs={'task_number' : i}
            )
            task.doc_md = """
                just prints iteration num
            """

        task