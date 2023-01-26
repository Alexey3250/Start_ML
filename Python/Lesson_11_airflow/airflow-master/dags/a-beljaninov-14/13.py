from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator


def determine_course():
    from airflow.models import Variable
    if Variable.get('is_startml') == 'True':
        return startml_desc.task_id
    return not_startml_desc.task_id

def print_startml_desc():
    print("StartML is a starter course for ambitious people")

def print_not_startml_desc():
    print("Not a startML course, sorry")


# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        'xcom_dag',
        start_date=datetime(2021, 1, 1),
        max_active_runs=2,
        schedule_interval=timedelta(minutes=30),
        default_args=default_args,
        catchup=False
) as dag:

    before_branching = DummyOperator(
        task_id='before_branching',
        dag=dag,
    )

    after_branching = DummyOperator(
        task_id='after_branching',
        dag=dag,
    )

    determine_course = BranchPythonOperator(
        task_id='determine_course',
        python_callable=determine_course,
    )

    startml_desc = PythonOperator(
        task_id='startml_desc',
        python_callable=print_startml_desc,
    )

    not_startml_desc = PythonOperator(
        task_id='not_startml_desc',
        python_callable=print_not_startml_desc,
    )

    before_branching >> determine_course
    determine_course >> startml_desc >> after_branching
    determine_course >> not_startml_desc >> after_branching
