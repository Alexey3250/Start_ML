from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable

with DAG(
        "hw_13_p-terentev-14",
        default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5)
        },
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 11, 11),
        catchup=False,
        tags=['DAG_13']
) as dag:
    def choice():
        if Variable.get('is_startml') == 'True':
            return "startml_desc"
        return "not_startml_desc"


    def startml_desc():
        print("StartML is a starter course for ambitious people")


    def not_startml_desc():
        print("Not a startML course, sorry")


    start = DummyOperator(
        task_id='start_branching'
    )

    a1 = BranchPythonOperator(
        task_id='choosen',
        python_callable=choice,
        trigger_rule='one_success'
    )

    a2 = PythonOperator(
        task_id='startml_desc',
        python_callable=startml_desc,
    )

    a3 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=not_startml_desc,
    )

    finish = DummyOperator(
        task_id='finish_branching'
    )

    start >> a1 >> [a2, a3] >> finish