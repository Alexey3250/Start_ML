from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

from datetime import datetime, timedelta

def is_startml():
    is_startml = Variable.get("is_startml")
    if is_startml == "True":
        return "startml_desc"
    else:
        return "not_startml_desc"

def startml_desc():
    print("StartML is a starter course for ambitious people")


def not_startml_desc():
    print("Not a startML course, sorry")

with DAG(
    's-sehova-17-13',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),

    catchup=False,
    tags=['task13'],
) as dag:
    date = "{{ds}}"
    
    dummy_step_1 = DummyOperator(
        task_id="step_1",
        trigger_rule="all_success",
    )
    branch = BranchPythonOperator(
        task_id = 'branch',
        python_callable = is_startml,
    )
    startml_desc = PythonOperator(
        task_id = 'startml_desc',
        python_callable = startml_desc,
    )
    not_startml_desc = PythonOperator(
        task_id = 'not_startml_desc',
        python_callable = not_startml_desc,
    )
    dummy_step_2 = DummyOperator(
        task_id="step_2",
        trigger_rule="all_success",
    )
        
dummy_step_1 >> branch >> [startml_desc, not_startml_desc] >> dummy_step_2