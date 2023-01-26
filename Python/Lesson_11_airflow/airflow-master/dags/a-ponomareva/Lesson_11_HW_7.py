"""
Добавьте в PythonOperator из второго задания kwargs
и передайте в этот kwargs task_number со значением переменной цикла.
Также добавьте прием аргумента ts и run_id в функции, указанной в PythonOperator, и распечатайте эти значения.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def print_task_num(task_number, ts, run_id):
    print(f'task number is: {task_number}', ts, run_id)


with DAG(
    'DAG_HW_7_ponomareva',
    default_args=default_args,
    description='DAG for HW_7',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['ponomareva'],
) as dag:

    for i in range(20):
        t2 = PythonOperator(
            task_id='print_task_num_' + str(i),
            python_callable=print_task_num,
            op_kwargs={'task_number': i},
        )
        t2.doc_md = dedent(
            """\
            ### `PythonOperator`
            Printing **number** of c_20_ commands
            with *python* func in cycle
            """
        )
