"""Homework_3 script"""

from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
        'hw_3_d-gusev',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='DAG for hw_3',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 8, 13),
        catchup=False,
        tags=['hw_3']
) as dag:
    for i in range(10):
        print_echo_count = BashOperator(
            task_id='print' + str(i),
            bash_command=f"echo {i}",
        )

    print_echo_count.doc_md = dedent(
        """
        Первые 10 задач сделайте типа BashOperator
        и выполните в них произвольную команду,
        так или иначе использующую переменную цикла
        (например, можете указать f"echo (i)")
        """
    )

    def print_task_number(task_number):
        print(f"task_number is: {task_number}")

    for i in range(10, 30):
        print_str_count = PythonOperator(
            task_id='print_task_number' + str(i),
            python_callable=print_task_number,
            op_kwargs={'task_number': i}
        )

    print_str_count.doc_md = dedent(
        """
        Оставшиеся 20 задач должны быть PythonOperator.
        Функция должна печатать task number is: (task_number),
        где task_number - номер задания из цикла.
        """
    )

    print_echo_count >> print_str_count
