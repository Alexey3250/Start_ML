from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from airflow.models import Variable


with DAG(
        's-duzhak-2-task_12',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='A simple tutorial DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 9, 11),
        catchup=False,
        tags=['example'],
) as dag:
    def first_function():
        print("StartML is a starter course for ambitious people")

    def branch():
        is_startml = Variable.get('is_startml')
        if is_startml == 'True':
            return 'startml_desc'
        else:
            return 'not_startml_desc'

    def is_startml_print():
        print("StartML is a starter course for ambitious people")


    def not_startml_print():
        print("Not a startML course, sorry")

    before_branching = PythonOperator(
        task_id='before_branching',
        python_callable=first_function
    )

    determine_course = BranchPythonOperator(
        task_id='determine_course',
        python_callable=branch
    )

    start_ml_desc = PythonOperator(
        task_id='startml_desc',
        python_callable=is_startml_print
    )

    not_start_ml_desc = PythonOperator(
        task_id='not_startml_desc',
        python_callable=not_startml_print
    )

    after_branching = DummyOperator(
        task_id='after_branching'
    )

    before_branching >> determine_course >> [start_ml_desc, not_start_ml_desc] >> after_branching





