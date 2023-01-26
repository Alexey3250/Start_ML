from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

with DAG(

    'HW_13_e-dracheva',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
        },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['HW_13_e-dracheva'],
    ) as dag:
    
    def choice():
        is_startml = Variable.get("is_startml")
        if is_startml == "True":
            return "startml_desc"
        else: 
            return "not_startml_desc"    
    
    def startml_desc():
        print("StartML is a starter course for ambitious people")
        
    def not_startml_desc():
        print("Not a startML course, sorry")
    
    t1 = DummyOperator(task_id="doing_nothing")
    
    t2 = BranchPythonOperator(task_id='choice', python_callable=choice)
    
    t3 = PythonOperator(
            task_id="startml_desc",
            python_callable=startml_desc)
    
    t4 = PythonOperator(
            task_id="not_startml_desc",
            python_callable=not_startml_desc)
    
    t5 = DummyOperator(task_id="ending")
    
    t1 >> t2 >> [t3, t4] >> t5