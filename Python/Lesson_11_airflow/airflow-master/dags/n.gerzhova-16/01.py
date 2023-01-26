from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'first',
    default_args={
        'depends_on_past': False,
        'email': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG for Task 01',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 7, 23),
) as dag:
    t1 = BashOperator(
        task_id='print_directory',
        bash_command='pwd',
    )

    def print_ds(ds, **kwargs):
        print(ds)

    t2 = PythonOperator(
        task_id='print_ds',
        python_callable=print_ds,
    )

    t1 >> t2