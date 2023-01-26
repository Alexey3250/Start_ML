"""Homework_7 script"""

from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
        'hw_7_d-gusev',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='DAG for hw_7',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 8, 13),
        catchup=False,
        tags=['hw_7']
) as dag:
    for i in range(10):
        print_echo_count = BashOperator(
            task_id='print' + str(i),
            bash_command=f"echo {i}",
        )

    print_echo_count.doc_md = dedent(
        """
         Также добавьте прием аргумента ts и run_id в функции,
         указанной в PythonOperator, и распечатайте эти значения.
        """
    )

    def print_task_number(task_number, ts, run_id):
        print(f"task_number is: {task_number}")
        print(ts)
        print(run_id)

    for i in range(10, 30):
        print_str_count = PythonOperator(
            task_id='print_task_number' + str(i),
            python_callable=print_task_number,
            op_kwargs={'task_number': i}
        )

    print_str_count.doc_md = dedent(
        """
        Добавьте в `PythonOperator` из второго задания
        (где создавали 30 операторов в цикле)
        kwargs и передайте в этот kwargs
        `task_number` со значением переменной цикла.
        """
    )

    print_echo_count >> print_str_count
