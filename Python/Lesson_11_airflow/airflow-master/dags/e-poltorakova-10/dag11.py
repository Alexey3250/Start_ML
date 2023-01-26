from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from textwrap import dedent
from datetime import datetime, timedelta
from airflow.models import Variable

with DAG(
    'hm_13_e-poltorakova',
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
    tags=['e-poltorakova'],

) as dag:

    def startml_value():
        if Variable.get("is_startml")  == 'True':
            task_id="startml_desc"
        else: 
            task_id="not_startml_desc"
        return task_id

    def startml_true():
        print("StartML is a starter course for ambitious people")
    
    def startml_false():
        print("Not a startML course, sorry")

    task1 = BranchPythonOperator(
        task_id= 'check_startml',
        python_callable=startml_value,
    )

    task2 = PythonOperator(
        task_id = 'startml_desc',
        python_callable=startml_true,
    )

    task3 = PythonOperator(
        task_id = 'not_startml_desc',
        python_callable=startml_false,
    )

    task1.doc_md = dedent(
        """
        Example of using BranchPythonOperator
        """
        )

    task1 >> [task2, task3]