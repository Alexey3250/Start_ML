from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator

with DAG(
    'm-zaminev_task_13',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='m-zaminev_task_13',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 11, 16),
    catchup=False,
    tags=['m-zaminev_task_13']

) as dag:

    def determine_course():
        var = Variable.get("is_startml")
        if var == "True":
            return "startml_desc"
        else:
            return "not_startml_desc"

    def startml():
        print("StartML is a starter course for ambitious people")

    def not_startml():
        print("Not a startML course, sorry")
    
    t1 = DummyOperator(
        task_id="before_branching"
    )
    
    t2 = BranchPythonOperator(
        task_id="determine_course",
        python_callable=determine_course
    )

    t3 = PythonOperator(
        task_id="startml_desc",
        python_callable=startml
    )

    t4 = PythonOperator(
        task_id="not_startml_desc",
        python_callable=not_startml
    )

    t5 = DummyOperator(
        task_id="after_branching"
    ) 

    t1 >> t2 >> [t3, t4] >> t5
