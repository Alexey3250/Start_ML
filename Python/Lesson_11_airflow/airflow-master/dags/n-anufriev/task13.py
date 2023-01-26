from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator


with DAG(
        'hw_13_n-anufriev',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='anufriev_lesson13',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 10, 31),
        catchup=False,
        tags=['hw_13_n-anufriev'],
) as dag:

    from airflow.operators.python import BranchPythonOperator

    STARTML_ID = "startml_desc"
    ANOTHER_COURSE_ID = "not_startml_desc"


    def choose_course():
        return STARTML_ID if Variable.get("is_startml") == "True" else ANOTHER_COURSE_ID

    t4 = DummyOperator(task_id="before_branching")

    branching = BranchPythonOperator(
        task_id="determine_course",
        python_callable=choose_course,
    )

    t_startml = PythonOperator(
        task_id=STARTML_ID,
        python_callable=lambda: "StartML is a starter course for ambitious people"
    )

    t_another_course = PythonOperator(
        task_id=ANOTHER_COURSE_ID,
        python_callable=lambda: "Not a startML course, sorry"
    )

    t5 = DummyOperator(task_id="after_branching")

    print_var = PythonOperator(
        task_id="print_var",
        python_callable=lambda: print(Variable.get("is_startml"))
    )

    t4 >> print_var >> branching >> [t_startml, t_another_course] >> t5
