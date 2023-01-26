from datetime import datetime, timedelta

from airflow import DAG

from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator


def branch_choice():
    if Variable.get("is_startml") is "True":
        return 'startml_desc'
    else:
        return 'not_startml_desc'


def left_task():
    print("StartML is a starter course for ambitious people")


def right_task():
    print("Not a startML course, sorry")


with DAG(
    'hw_13_a-astakhov',
    # Параметры по умолчанию
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime.now(),
    catchup=False,
    tags=['hw_13_a-astakhov'],
) as dag:
    t_first = DummyOperator(
        task_id='before_branching',
    )
    
    branching = BranchPythonOperator(
        task_id='determine_course',
        python_callable=branch_choice
    )

    t_left = PythonOperator(
        task_id="startml_desc",
        python_callable=left_task
    )

    t_right = PythonOperator(
        task_id="not_startml_desc",
        python_callable=right_task
    )

    t_last = DummyOperator(
        task_id='after_branching'
    )

    t_first >> branching >> [t_left, t_right] >> t_last
