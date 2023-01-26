from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
        'HW_4_d-lobachev_Dynamic_task',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='DAG step 4 of HW Lesson 11',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 2, 10),
        catchup=False,
        tags=['lobachev'],
) as dag:
    def print_task(task_number):
        print(f'task number is: {task_number}')


    for i in range(1, 31):
        if i < 11:
            bash_op = BashOperator(
                task_id='print_task' + str(i),
                bash_command=f'echo {i}'
            )
            bash_op.doc_md = dedent(
                """\
                ### `BashOperator`
                Printing **number** of _10_ commands
                with *echo* command in cycle
                """
            )
        else:
            python_op = PythonOperator(
                task_id='task_number_' + str(i),
                python_callable=print_task,
                op_kwargs={'task_number': i}
            )
            python_op.doc_md = dedent(
                """\
                ### `PythonOperator`
                Printing **number** of c_20_ commands
                with *python* func in cycle
                """
            )

    bash_op >> python_op
