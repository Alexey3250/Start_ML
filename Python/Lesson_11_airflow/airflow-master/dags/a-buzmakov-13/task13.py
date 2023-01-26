from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable

def get_decision():
    if Variable.get("is_startml") == "True":
        return "startml_desc"
    else:
        return "not_startml_desc"


def get_info_startml():
    print("StartML is a starter course for ambitious people")


def get_info_not_startml():
    print("Not a startML course, sorry")



with DAG(
    'a-buzmakov-13_task_13',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description='a-buzmakov-13_DAG_task13',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 10, 20),
    catchup=False,
    tags=['task_13'],
) as dag:
    a1 = DummyOperator(
        task_id='dummy_start'
    )
    a2 = BranchPythonOperator(
        task_id='get_decision',  
        python_callable=get_decision
    )
    a3 = PythonOperator(
        task_id='startml_desc',  
        python_callable=get_info_startml
    )
    a4 = PythonOperator(
        task_id='not_startml_desc',  
        python_callable=get_info_not_startml
    )
    a5 = DummyOperator(
        task_id='dummy_finish'
    )

    a1 >> a2 >> [a3, a4] >> a5

