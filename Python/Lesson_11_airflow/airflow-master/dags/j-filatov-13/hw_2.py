from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_2_j-filatov-13',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='This is a frist excercise. BashOperator and PythonOperator',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 10, 21),
    catchup=False,
    tags=['hw_2_fyurikon@gmail.com'],
) as dag:

    t1 = BashOperator(
        task_id='bo_print_pwd',
        bash_command='pwd',
    )

    def print_ds(ds):
        print(ds)
        return 'I printed ds successfully!'

    t2 = PythonOperator(
        task_id='po_print_ds',
        python_callable=print_ds,
    )

    t1 >> t2
