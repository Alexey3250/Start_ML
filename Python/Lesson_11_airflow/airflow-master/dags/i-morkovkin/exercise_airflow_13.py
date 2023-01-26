from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from textwrap import dedent
from airflow.providers.postgres.operators.postgres import PostgresHook

with DAG(
        'i-morkovkin_hw_13',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='Exercise No.13',
        start_date=datetime(2022, 6, 15)
) as dag:

    def branch_cause_variable():
        from airflow.models import Variable

        is_start_ml = Variable.get("is_startml")
        if is_start_ml.lower() == 'true':
            return 'startml_desc'
        else:
            return 'not_startml_desc'

    def print_for_startml():
        print("StartML is a starter course for ambitious people")

    def print_not_for_startml():
        print("Not a startML course, sorry")

    branching = BranchPythonOperator(
        task_id="determine_course",
        python_callable=branch_cause_variable
    )

    t1 = PythonOperator(
        task_id="startml_desc",
        python_callable=print_for_startml
    )

    t2 = PythonOperator(
        task_id="not_startml_desc",
        python_callable=print_not_for_startml
    )

    t_start = DummyOperator(
        task_id="before_branching"
    )

    t_end = DummyOperator(
        task_id="after_branching"
    )

    t_start >> branching >> [t1, t2] >> t_end
