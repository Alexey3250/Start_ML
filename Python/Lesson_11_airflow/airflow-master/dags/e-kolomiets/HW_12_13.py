from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator


def get_task_id():
    from airflow.models import Variable
    if Variable.get('is_startml') == 'True':
        return 'startml_desc'
    else:
        return 'not_startml_desc'


def result1():
    print('StartML is a starter course for ambitious people')


def result2():
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
        'kolomiets_branching',
        default_args=default_args,
        start_date=datetime(2021, 12, 22),
        schedule_interval=timedelta(days=1)
) as dag:
    chooser = BranchPythonOperator(
        task_id='determine_course',
        python_callable=get_task_id
    )
    startml_desc = PythonOperator(
        task_id='startml_desc',
        python_callable=result1
    )
    not_startml_desc = PythonOperator(
        task_id='not_startml_desc',
        python_callable=result2
    )
    first_dummy = DummyOperator(
        task_id='before_branching'
    )
    last_dummy = DummyOperator(
        task_id='after_branching'
    )
    first_dummy >> chooser >> [startml_desc, not_startml_desc] >> last_dummy
