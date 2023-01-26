"""
Создайте DAG, имеющий BranchPythonOperator. Логика ветвления должна быть следующая:
если значение Variable is_startml равно "True", то перейти в таску с task_id="startml_desc",
иначе перейти в таску с task_id="not_startml_desc". Затем объявите две задачи с task_id="startml_desc" и task_id="not_startml_desc".
В первой таске распечатайте "StartML is a starter course for ambitious people", во второй "Not a startML course, sorry".
Перед BranchPythonOperator можете поставить DummyOperator
"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable

from datetime import datetime, timedelta


def choosing_task():
    from airflow.models import Variable
    is_startml = Variable.get('is_startml')
    if is_startml=='True': return 'startml_desc'
    else: return 'not_startml_desc'


def task1():
    print('StartML is a starter course for ambitious people')


def task2():
    print('Not a startML course, sorry')

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'DAG_HW_13_ponomareva',
    default_args=default_args,
    description='DAG for HW_13',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 1),
    catchup=False,
    tags=['ponomareva'],
) as dag:
    branch_task = BranchPythonOperator(
        task_id='choosing_task',
        python_callable=choosing_task,
    )
    t1 = PythonOperator(
        task_id='startml_desc',
        python_callable=task1,
    )
    t2 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=task2,
    )
    t0 = DummyOperator(
        task_id='start',
    )
    t_end = DummyOperator(
        task_id = 'finish',
    )

    t0 >> branch_task >> [t1, t2] >> t_end