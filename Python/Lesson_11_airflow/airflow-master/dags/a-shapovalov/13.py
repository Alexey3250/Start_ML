from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.models import Variable


def choose_print():
    is_startml = Variable.get('is_startml')
    if is_startml == 'True':
        return 'startml_desc'
    return 'not_startml_desc'


def print_startml():
    print('StartML is a starter course for ambitious people')


def print_not_startml():
    print('Not a startML course, sorry')


with DAG(
        'hw_13_a-shapovalov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
        description='Excercise 13 a-shapovalov',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['hw_13_a-shapovalov']
) as dag:

    branch = BranchPythonOperator(
        task_id='choose_print',
        python_callable=choose_print
    )

    task_startml = PythonOperator(
        task_id="startml_desc",
        python_callable=print_startml
    )

    task_not_startml = PythonOperator(
        task_id="not_startml_desc",
        python_callable=print_not_startml
    )

    branch >> [task_startml, task_not_startml]
