from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator


def is_start_ml():
    from airflow.models import Variable
    res = Variable.get('is_startml')
    if res == 'True':
        return 'startml_desc'
    else:
        return 'not_startml_desc'


def print_startml():
    print('StartML is a starter course for ambitious people')


def print_not_startml():
    print('Not a startML course, sorry')



with DAG(
        'HW_11_d-lobachev_Connections',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='DAG step 11 of HW Lesson 11',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 2, 10),
        catchup=False,
        tags=['lobachev'],
) as dag:
    t1 = DummyOperator(
        task_id='before_branching'
    )
    t2 = BranchPythonOperator(
        task_id='determine_course',
        python_callable=is_start_ml
    )
    t3 = PythonOperator(
        task_id='startml_desc',
        python_callable=print_startml
    )
    t4 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=print_not_startml
    )
    t5 = DummyOperator(
        task_id='after_branching'
    )

    t1 >> t2 >> [t3, t4] >> t5


