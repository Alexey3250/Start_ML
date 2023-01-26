"""
Test documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable


def is_ml():
    if Variable.get('is_startml') == 'True':
        return 'startml_desc'
    else:
        return 'not_startml_desc'

def print_true():
    print('StartML is a starter course for ambitious people')

def print_false():
    print('Not a startML course, sorry')


with DAG(
        'hw_13_bartenev',
        default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)},
        description='A simple tutorial DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['example'],
) as dag:
    t0 = DummyOperator(task_id='before')

    tb = BranchPythonOperator(
        task_id='branch_is_ml',
        python_callable=is_ml
    )

    t1 = PythonOperator(task_id='startml_desc', python_callable=print_true)
    t2 = PythonOperator(task_id='not_startml_desc', python_callable=print_false)

    t1000 = DummyOperator(task_id='fin')

    t0 >> tb >> [t1, t2] >> t1000