from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

with DAG(
        'bogdan_romanov_task_7',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='hw_2_b-romanov-14',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 11, 11),
        catchup=False,
        tags=['b-b']

) as dag:
    for i in range(10):
        run_bash = BashOperator(
            task_id=f'show_{i}',
            bash_command=f"echo {i}"
    )

    def print_task_number(task_number, ts, run_id):
        print(f'task number is: {task_number, ts, run_id}')

    for i in range(20):
        run_python = PythonOperator(
            task_id='task_number' + str(i),
            python_callable=print_task_number,
            op_kwargs={'task_number': i}
        )

    run_python.doc_md = dedent(
        """
        This document can print *numbers* by running `code`
        # The output is string with **number** 
        """
    )

    run_bash >> run_python