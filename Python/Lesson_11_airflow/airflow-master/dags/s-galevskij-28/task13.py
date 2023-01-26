from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable

def is_startml():
    if Variable.get('is_startml') == 'True':
        return 'startml_desc'
    else:
        return 'not_startml_desc'

def startml_desc():
    return print('StartML is a starter course for ambitious people')

def not_startml_desc():
    return print('Not a startML course, sorry')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
    }

with DAG(
    'task12_galevskii',
    start_date=datetime(2021, 1, 1),
    max_active_runs=2,
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    catchup=False
) as dag:
    t0 = DummyOperator(task_id='before_branching')

    t1 = BranchPythonOperator(
        task_id='is_startml',
        python_callable=is_startml
    )
    t2 = PythonOperator(
        task_id='startml_desc',
        python_callable=startml_desc
    )
    t3 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=not_startml_desc
    )
    t4 = DummyOperator(task_id='after_branching')

    t0 >> t1 >> [t2, t3] >> t4