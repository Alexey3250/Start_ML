import os.path
from datetime import timedelta, datetime
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator

from airflow.models import Variable

dag_name = os.path.basename(__file__).rstrip('.py') + '_d-gavlovskij'

with DAG(
    dag_name,
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='somedag',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 10, 31),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['gavlique']
) as dag:
    def which_course():
        if Variable.get('is_startml') == 'True':
            return "startml_desc"
        else:
            return "not_startml_desc"

    def print_message(task_id):
        if task_id == 'startml_desc':
            print('StartML is a starter course for ambitious people')
        else:
            print('Not a startML course, sorry')

    t1 = DummyOperator(
        task_id='before_branching'
    )

    t2 = BranchPythonOperator(
        task_id='determine_course',
        python_callable=which_course,
        provide_context=True,
        dag=dag
    )

    t3 = PythonOperator(
        task_id="startml_desc",
        python_callable=print_message,
        dag=dag
    )

    t4 = PythonOperator(
        task_id="not_startml_desc",
        python_callable=print_message,
        dag=dag
    )

    t5 = DummyOperator(
        task_id='after_branching'
    )

    t1 >> t2
    t2 >> [t3, t4]
    t4 >> t5
    t3 >> t5
