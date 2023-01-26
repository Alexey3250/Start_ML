from datetime import datetime, timedelta

from airflow.operators.dummy import DummyOperator
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator, PythonOperator


def valiable_get():
    is_startml = Variable.get('is_startml')
    print(is_startml)


def branch_desicion(is_startml):
    if Variable.get('is_startml') == 'True':
        return 'startml_desc'
    else:
        return 'not_startml_desc'


def positive_task():
    print('StartML is a starter course for ambitious people')


def negative_task():
    print('Not a startML course, sorry')


with DAG(
        'task_13',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='Airflow branching',
        start_date=datetime(2022, 6, 15),
        schedule_interval=timedelta(days=1),
        catchup=False,
        tags=['e.zabuzov', 'step_13']
) as dag:
    before_branching = DummyOperator(
        task_id='Before_branching', )

    chosing_branch = BranchPythonOperator(
        task_id='chose_branch',
        python_callable=branch_desicion)

    t1 = PythonOperator(
        task_id="startml_desc",
        python_callable=positive_task)

    t2 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=negative_task)

    after_branching = DummyOperator(
        task_id='After_branching')

    before_branching >> chosing_branch >> [t1, t2] >> after_branching