from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

from airflow.models import Variable

with DAG(
    'dag11_g-volosnyh',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='dag11',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    t1 = DummyOperator(
        task_id='before_branching',
    )

    def course_branching():
        is_startml = Variable.get("is_startml")
        if is_startml == "True":
            return 'startml_desc'
        else:
            return 'not_startml_desc'


    t2 = BranchPythonOperator(
        task_id='determine_course',
        python_callable=course_branching,
    )

    def startml_func():
        print("StartML is a starter course for ambitious people")

    t3 = PythonOperator(
        task_id='startml_desc',
        python_callable=startml_func,
    )

    def not_startml_func():
        print("Not a startML course, sorry")

    t4 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=not_startml_func,
    )

    t5 = DummyOperator(
        task_id='after_branching',
    )

    t1 >> t2 >> [t3, t4] >> t5
