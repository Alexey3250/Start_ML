from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.operators.python import BranchPythonOperator
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator

def get_var():
    is_startml = Variable.get("is_startml")
    if is_startml == "True":
        return "startml_desc"
    else:
        return "not_startml_desc"
    
def print_t1():
    print("StartML is a starter course for ambitious people")

def print_t2():
    print("Not a startML course, sorry")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'hw_13_mardyshkin',
    default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
    },
    start_date=datetime(2022, 12, 26)
) as dag:
    determine_course = BranchPythonOperator(
        task_id = 'determine_course',
        python_callable = get_var
    )
    
    t1 = PythonOperator(
        task_id = 'startml_desc',
        python_callable = print_t1
    )
    
    t2 = PythonOperator(
        task_id = 'not_startml_desc',
        python_callable = print_t2
    )
    
    before_branching = DummyOperator(task_id='before_branching')
    
    after_branching = DummyOperator(task_id='after_branching')
    
    before_branching >> determine_course >> [t1, t2] >> after_branching
