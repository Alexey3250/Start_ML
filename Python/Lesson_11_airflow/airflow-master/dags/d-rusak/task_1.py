from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_context(ds, **kwargs):
    print(kwargs)
    print(ds)


with DAG(
    'first_dag_drusak',
    default_args={
        'depends_on_past': False,
        'email': ['rusakda@mail.ru'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['first_dag_drusak'],
) as dag:

    t1 = BashOperator(
        task_id='print_date',
        bash_command='pwd',
    )

    t2 = PythonOperator(
        task_id='print_the_context',
        python_callable=print_context,
    )

    t1 >> t2
