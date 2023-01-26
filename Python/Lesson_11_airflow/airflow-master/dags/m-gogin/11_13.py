from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import timedelta, datetime
from airflow.operators.dummy import DummyOperator


with DAG(
        'hw_13_m-gogin',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),

        },
        description='hw_13_m-gogin',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 7, 25),
        catchup=False,
        tags=['hw_13'],
) as dag:

    def print_var():
        from airflow.models import Variable
        if Variable.get('is_startml') == 'True':
            return "startml_desc"
        return "not_startml_desc"

    def startml_desc():
        print("StartML is a starter course for ambitious people")

    def not_startml_desc():
        print("Not a startML course, sorry")


    t1 = DummyOperator(
        task_id='11_13op'
    )

    t2 = BranchPythonOperator(
        task_id='determine_course',
        python_callable=print_var,
        trigger_rule='all_done'
    )

    t3 = PythonOperator(
        task_id='startml_desc',
        python_callable=startml_desc,
    )

    t4 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=not_startml_desc,
    )

    t5 = DummyOperator(
        task_id='11_13cl'
    )

    t1 >> t2 >> [t3, t4] >> t5


