from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta


def is_start_ml():
    if Variable.get('is_startml') == 'True':
        return 'startml_desc'
    else:
        return 'not_startml_desc'


def print_startml():
    print('StartML is a starter course for ambitious people')


def print_not_startml():
    print('Not a startML course, sorry')


with DAG(
    'hw_11_a-telesh',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='HW 12',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 22),
    catchup=False,
    tags=['At']
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
