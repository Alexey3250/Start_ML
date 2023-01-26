from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from psycopg2.extras import RealDictCursor
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable


def determine_course():
    switcher = Variable.get("is_startml")

    if switcher == 'True':
        return "startml_desc"
    return "not_startml_desc"


def startml_desc():
    print("StartML is a starter course for ambitious people")


def not_startml_desc():
    print("Not a startML course, sorry")


with DAG(
        'hw_13_j-filatov-13',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='hw_13_exercise_13',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 10, 22),
        catchup=False,
        tags=['hw_13_j-filatov-13_tag']
) as dag:
    t1 = DummyOperator(
        task_id='before_branching'
    )

    t2 = BranchPythonOperator(
        task_id="determine_course",
        python_callable=determine_course,
    )

    t3 = PythonOperator(
        task_id="startml_desc",
        python_callable=startml_desc,
    )

    t4 = PythonOperator(
        task_id="not_startml_desc",
        python_callable=not_startml_desc,
    )

    t5 = DummyOperator(
        task_id="after_branching"
    )

    t1 >> t2 >> [t3, t4] >> t5
