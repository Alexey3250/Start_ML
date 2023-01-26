from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import timedelta, datetime

with DAG(
    'first_dag',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='first dag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 12, 18),
    catchup=False,
    tags=['first_dag']
) as dag:

    t1 = BashOperator(
        task_id='show_directory',
        bash_command='pwd',
    )

    def print_date(ds):
        print(ds)


    t2 = PythonOperator(
        task_id='print_date',
        python_callable=print_date,
    )

    t1 >> t2
