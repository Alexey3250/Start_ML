from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.models import Variable
from datetime import timedelta, datetime

with DAG(
    '11_13_mishin',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='hw 13',
    schedule_interval=timedelta(days=7),
    start_date=datetime(2022, 9, 21),
    catchup=False,
    tags=['m-mishin']
) as dag:
    t1 = DummyOperator(
        task_id='before_branching',
    )

    def branching_rule():
        choice = Variable.get("is_startml")
        if choice == "True":
            return "startml_desc"
        else:
            return "not_startml_desc"

    t2 = BranchPythonOperator(
        task_id="branching",
        python_callable=branching_rule,
    )
    def print_start():
        print("StartML is a starter course for ambitious people")

    t3 = PythonOperator(
        task_id="startml_desc",
        python_callable=print_start
    )
    def print_not_start():
        print("Not a startML course, sorry")

    t4 = PythonOperator(
        task_id="not_startml_desc",
        python_callable=print_not_start
    )

    t5 = DummyOperator(
        task_id='after_branching'
    )

    t1 >> t2
    t2 >> t3
    t2 >> t4
    t3 >> t5
    t4 >> t5
