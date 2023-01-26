from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}


def choose_course():
    from airflow.models import Variable
    needed_value = Variable.get("is_startml")
    if needed_value == 'True':
        return "start_ml_desc"
    else:
        return "not_start_ml_desc"


def start_ml_print():
    print("StartML is a starter course for ambitious people")


def not_start_ml_print():
    print("Not a startML course, sorry")


with DAG(
        dag_id="task13_final_v-saharov-20",
        default_args=default_args,
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False
) as dag:
    before_branching = DummyOperator(task_id="before_branching")
    determine_course = BranchPythonOperator(task_id="determine_course", python_callable=choose_course)
    start_ml_desc = PythonOperator(task_id="start_ml_desc", python_callable=start_ml_print)
    not_start_ml_desc = PythonOperator(task_id="not_start_ml_desc", python_callable=not_start_ml_print)
    after_branching = DummyOperator(task_id="after_branching")
    
    before_branching >> determine_course >> [start_ml_desc, not_start_ml_desc] >> after_branching

