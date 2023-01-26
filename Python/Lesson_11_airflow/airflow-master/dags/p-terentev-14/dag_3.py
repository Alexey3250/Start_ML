from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw3_p-terentev-14',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
    ,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['dag_3']
) as dag:

    for i in range(10):
        a1 = BashOperator(
            task_id=f'task_number_is_{i}',
            bash_command=f'print({i})')
        a1.doc_md = dedent(
            """
            #### Task Documentation (BashOperator)
            элементы кода (заключены в кавычки `code`),
            **полужирный**, *курсив*
            # абзац (неполный)
            """
        )


    def print_task_number(task_number):
        print(f"task number is: {task_number}")


    for i in range(20):
        a2 = PythonOperator(
            task_id=f'print_task_num_{i}',
            python_callable=print_task_number,
            op_kwargs={'task_number': i}
		)
        a2.doc_md = dedent(
            """
            #### Task Documentation (PythonOperator)
            элементы кода (заключены в кавычки `code`),
            **полужирный**, *курсив*
            # абзац (неполный)
            """
        )
    a1 >> a2