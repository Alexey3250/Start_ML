from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.models import Variable


def _choose_way():
    is_startml = Variable.get("is_startml")
    if is_startml == 'True':
        return "startml_desc"
    return "not_startml_desc"

def print_startml_desc():
    print("StartML is a starter course for ambitious people")

def print_not_startml_desc():
    print("Not a startML course, sorry")

with DAG(
    'a-grohovskaja-9_hw_13',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
    description='First DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False
) as dag:

    choose_way = BranchPythonOperator(
        task_id = "choose_your_way",
        python_callable=_choose_way
    )

    t1 = PythonOperator(
        task_id = "startml_desc",
        python_callable = print_startml_desc
    )

    t2 = PythonOperator(
        task_id = "not_startml_desc",
        python_callable = print_not_startml_desc
    )

    choose_way >> [t1,t2]