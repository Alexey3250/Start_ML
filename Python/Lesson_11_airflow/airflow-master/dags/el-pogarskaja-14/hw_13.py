from airflow import DAG
from textwrap import dedent
from airflow.operators.dummy import DummyOperator
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator

with DAG(
        'el-pogarskaja-14_13',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
            },
        description='A simple tutorial DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2021, 1, 1),
        catchup=False,
        ) as dag:

    #function that gets and returns veriable is_startml from airflow
    #and then returns the number of the task to be executed depending on the variable meaning
    def choose_the_task():
        from airflow.models import Variable
        is_startml=Variable.get("is_startml") #gets variable is_startml from Airflow
        if is_startml == True:
            return 'startml_desc'
        else:
            return 'not_startml_desc'
    #DummyOperator -- just for nice look
    Dum = DummyOperator(
            task_id='dum',
            )

    #tasl which determines branching
    branching = BranchPythonOperator(
            task_id='branching',
            python_callable=choose_the_task,
            )
    #one more DummyOperator
    my = DummyOperator(
            task_id='my',
            )
    #function for t1
    def one():
        print('StartML is a starter course for ambitious people')

    t1 = PythonOperator(
            task_id='startml_desc',
            python_callable=one,
            )
    #function for t2
    def two():
        print('Not a startML course, sorry')
    t2 = PythonOperator(
            task_id='not_startml_desc',
            python_callable=two,
            )

