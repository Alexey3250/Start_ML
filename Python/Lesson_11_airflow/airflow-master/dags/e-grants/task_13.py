from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable


with DAG (
    'task_13_e-grants',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
},
    description='DAG for task_13',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 10, 22),
    catchup=False,
    tags=['task_13'],
) as dag:
    
    def get_task_id():
        is_startml = Variable.get("is_startml")
        if is_startml == 'True':
            return "startml_desc" 
        else:
            return "not_startml_desc"
        
    
    def startml_desc():
        print("StartML is a starter course for ambitious people")

    def not_startml_desc():
        print("Not a startML course, sorry")
            
    task_1 = DummyOperator(task_id = 'before_branching')
    
    task_2 = BranchPythonOperator(task_id = 'determine_course', python_callable = get_task_id)
    
    task_3 = PythonOperator(task_id = 'startml_desc', python_callable = startml_desc)
    
    task_4 = PythonOperator(task_id = 'not_startml_desc', python_callable = not_startml_desc)
    
    task_5 = DummyOperator(task_id='after_branching')
    
    
    task_1 >> task_2 >> [task_3, task_4] >> task_5