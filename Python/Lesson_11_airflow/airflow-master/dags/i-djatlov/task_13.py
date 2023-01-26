from textwrap import dedent
from datetime import datetime, timedelta
from textwrap import dedent


from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator

with DAG(
    'hw_13_i-djatlov',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description='task_13',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 11, 1),
    catchup=False,
    tags=['i-djatlov']
) as dag:
    def if_var():
        from airflow.models import Variable
        var = Variable.get("is_startml")
        if is_startml == "True":
            task_id="start_ml_desc"
        else:
            task_id="not_startml_desc"
    def pr1():
        print("StartML is a starter course for ambitious people")
    def pr2():
        print("Not a startML course, sorry")
            
    t1 = PythonOperator(
        task_id='start_ml_desc',
        python_callable=pr1,
    )
        
    t2 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=pr2,
    )
    
    first = DummyOperator(task_id='before_branching')
    
    branching = BranchPythonOperator(
        task_id='determine_course',
        python_callable=if_var,
    )
    
    end = DummyOperator(task_id='after_branching')
    
    first >> branching >> end