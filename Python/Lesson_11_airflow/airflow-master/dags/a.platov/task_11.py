from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

def is_startml():
    from airflow.models import Variable
    is_startml = Variable.get("is_startml")
    if is_startml == "True":
        return "startml_desc"
    else:
        return "not_startml_desc"

def print_string(s):
    print(s)
    

with DAG(
    'HW_13_a.platov',
    default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),         
        },
        description='Home Work N11 -- BranchPythonOperator',
        start_date=datetime(2022, 7, 24),
        catchup=False,
        tags=['a.platov'],
    ) as dag:
           
        dummy_1 = DummyOperator(
                task_id="start",
                trigger_rule="all_success",
                )    

        dummy_2 = DummyOperator(
                task_id="end",
                trigger_rule="all_success",
                )

        branch = BranchPythonOperator(
                task_id="branch",
                python_callable=is_startml,
                dag=dag,
                )
        t1 = PythonOperator(
            task_id='startml_desc',
            python_callable=print_string,
            op_kwargs={'s': "StartML is a starter course for ambitious people"}
            )
        
        t2 = PythonOperator(
            task_id='not_startml_desc',
            python_callable=print_string,
            op_kwargs={'s': "Not a startML course, sorry"}
            )
        
        dummy_1 >> branch >> [t1, t2] >> dummy_2
