from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator

with DAG(
    'j-jancharskaja_12',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description = 'My tenth DAG',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 11, 11),
    catchup = False,
    tags = ['tenth']
) as dag:

    t1 = DummyOperator(
        task_id = 'before_branching'
        )


    def check_var():
        from airflow.models import Variable

        is_startml = Variable.get('is_startml')
        if is_startml == 'True':
            return 'startml_desc'
        else:
            return 'not_startml_desc'
    
    t2 = BranchPythonOperator(
        task_id = 'determine_course',
        python_callable = check_var
    )


    def print_if_true():
        print("StartML is a starter course for ambitious people")

    t3 = PythonOperator(
        task_id = 'startml_desc',
        python_callable = print_if_true
    )


    def print_if_false():
        print("Not a startML course, sorry")

    t4 = PythonOperator(
        task_id = 'not_startml_desc',
        python_callable = print_if_false
    )


    t5 = DummyOperator(
        task_id = 'after_branching'
        )


    t1 >> t2 >> [t3, t4] >> t5