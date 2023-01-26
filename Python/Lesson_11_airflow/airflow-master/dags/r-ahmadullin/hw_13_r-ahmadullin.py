from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator


def get_condition():
    from airflow.models import Variable
    if Variable.get('is_startml') == 'True':
        return "startml_desc"
    return "not_startml_desc"


with DAG(
        'task13',
        # Параметры по умолчанию для тасок
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='task 13',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 5),
        catchup=False,
        tags=['task_13'],
) as dag:
    def startml_desc():
        print("StartML is a starter course for ambitious people")


    def not_startml_desc():
        print("Not a startML course, sorry")


    t1 = PythonOperator(
        task_id='startml_desc',
        python_callable=startml_desc,
    )

    t2 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=not_startml_desc,
    )

    t3 = BranchPythonOperator(
        task_id='determine_course',
        python_callable=get_condition,
        trigger_rule='all_done'
    )

    start = DummyOperator(
        task_id='before_branching'
    )

    finish = DummyOperator(
        task_id='after_branching'
    )

    start >> t3 >> [t1, t2] >> finish