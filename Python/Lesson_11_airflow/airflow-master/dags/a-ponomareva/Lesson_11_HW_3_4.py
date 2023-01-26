"""
Создайте новый DAG и объявите в нем 30 задач. Первые 10 задач сделайте типа BashOperator и выполните в них произвольную команду,
так или иначе использующую переменную цикла (например, можете указать f"echo {i}").

Оставшиеся 20 задач должны быть PythonOperator, при этом функция должна задействовать переменную из цикла.
Вы можете добиться этого, если передадите переменную через op_kwargs и примете ее на стороне функции.
Функция должна печатать "task number is: {task_number}", где task_number - номер задания из цикла.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent


default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def print_task_num(task_number):
    print(f'task number is: {task_number}')

with DAG(
    'DAG_HW_3_4_ponomareva',
    default_args=default_args,
    description='DAG for HW_3_4',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['ponomareva'],
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id='print_command_' + str(i),
            bash_command='echo {i}',
        )

        t1.doc_md = dedent(
            """\
            ### `BashOperator`
            Printing **number** of _10_ commands
            with *echo* command in cycle
            """
        )
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

    t1 >> t2