from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

def branching():
    is_startml = Variable.get("is_startml")

    if is_startml == "True":
        return "startml_desc"

    return "not_startml_desc"

def startml_desc():
    print("StartML is a starter course for ambitious people")

def not_startml_desc():
    print("Not a startML course, sorry")

with DAG(
    'HW_13_s-dzhumaliev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime(2022, 1, 1),
) as dag:
    branching_task = BranchPythonOperator(
        task_id='branching',
        python_callable=branching
    )
    t2 = PythonOperator(
        task_id='startml_desc',
        python_callable=startml_desc
    )
    t3 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=not_startml_desc
    )
    dummy_start_task = DummyOperator(
        task_id='before_branching',
    )
    dummy_end_task = DummyOperator(
        task_id='after_branching',
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    dummy_start_task >> branching_task >> [t2, t3] >> dummy_end_task
