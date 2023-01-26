from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator


def branch_func(**kwargs):
    is_started = Variable.get("is_startml")
    if is_started == "True":
        return 'startml_desc'
    else:
        return 'not_startml_desc'


def print_started():
    print("StartML is a starter course for ambitious people")


def print_not_started(ti):
    print("Not a startML course, sorry")


with DAG(
        's_pletnev_task_13',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='task_13_dag',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 10, 21),
        catchup=False,
        tags=['task_13'],
) as dag:
    print_started_task = PythonOperator(
        task_id = 'startml_desc',
        python_callable=print_started,
    )
    print_not_started_task = PythonOperator(
        task_id = 'not_startml_desc',
        python_callable=print_not_started,
    )
    branch_op = BranchPythonOperator(
        task_id='branch_task',
        provide_context=True,
        python_callable=branch_func,
    )
    before = DummyOperator(
        task_id='before_branching',
    )
    after = DummyOperator(
        task_id='after_branching',
    )

    before >> branch_op >> [print_started_task, print_not_started_task] >> after
