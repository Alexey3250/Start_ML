from datetime import datetime, timedelta
from textwrap import dedent
from airflow.models import Variable

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator

from airflow.providers.postgres.operators.postgres import PostgresHook


with DAG(
    'hw_13_p-tarabanov',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    
    description='13 task',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 9, 11),
    catchup=False,
    tags=['BranchPythonOperator'],
) as dag:
    
    task0 = DummyOperator(
        task_id = 'before_branching'
    )
    
    def determine_course():       
        if Variable.get("is_startml")=="True":
            task_id = 'startml_desc'
        elif Variable.get("is_startml")=="False":
            task_id = 'not_startml_desc'     
        return task_id
    
    task1 = BranchPythonOperator(
        task_id = 'determine_course',
        python_callable = determine_course
    )
    
    def def_startml_desc():
        print('StartML is a starter course for ambitious people')
    
    task2 = PythonOperator(
        task_id = 'startml_desc',
        python_callable = def_startml_desc
    )
    
    def def_not_startml_desc():
        print('Not a startML course, sorry')
    
    task3 = PythonOperator(
        task_id = 'not_startml_desc',
        python_callable = def_not_startml_desc
    )
    
    task4 = DummyOperator(
        task_id = 'after_branching'
    )
    
    task0 >> task1 >> [task2, task3] >> task4
    