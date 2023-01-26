from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from textwrap import dedent
from airflow.hooks.base_hook import BaseHook
from psycopg2.extras import RealDictCursor
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresHook
from airflow.operators.dummy import DummyOperator




def print_startml():
    print("StartML is a starter course for ambitious people")

def dont_print_startml():
    print("Not a startML course, sorry")


def choose_dag():
    from airflow.models import Variable
    if Variable.get("is_startml") == "True":
        return "startml_desc"
    return "not_startml_desc"



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'hw_13_de-jakovlev',
    start_date=datetime(2021, 1, 1),
    max_active_runs=2,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
    catchup=False
) as dag:

    dummy_begin = DummyOperator(
        task_id='before_branching',
    )

    p1 = BranchPythonOperator(
        task_id="determine_course",
        python_callable=choose_dag
    )
    p2 = PythonOperator(
        task_id="not_startml_desc",
        python_callable=dont_print_startml
    )
    p3 = PythonOperator(
        task_id="startml_desc",
        python_callable=print_startml
    )

    dummy_end = DummyOperator(
        task_id='after_branching',
    )

    dummy_begin >> p1 >> [p2, p3] >> dummy_end






