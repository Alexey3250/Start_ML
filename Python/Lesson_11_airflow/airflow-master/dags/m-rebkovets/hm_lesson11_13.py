from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator


def branch_func():
    from airflow.models import Variable
    if Variable.get("is_startml") == "True":
        return "startml_desc"
    return "not_startml_desc"

def start_ml():
    print("StartML is a starter course for ambitious people")

def not_start_ml():
    print("Not a startML course, sorry")


with DAG(
    '11_13_rbkvts',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
    description='hw_13',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 7, 22),
    catchup=False,
    tags=['11_2'],
) as dag:

    start = DummyOperator(
        task_id='before_branching',
    )

    branch = BranchPythonOperator(
        task_id='determine_class',
        python_callable=branch_func,
    )

    startml_desc = PythonOperator(
        task_id='startml_desc',
        python_callable=start_ml,
    )

    not_startml_desc= PythonOperator(
        task_id='not_startml_desc',
        python_callable=not_start_ml,
    )

    after = DummyOperator(task_id='after_branching')


    start >> branch >> [startml_desc, not_startml_desc] >> after