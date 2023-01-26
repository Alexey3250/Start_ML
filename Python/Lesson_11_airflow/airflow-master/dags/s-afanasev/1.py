from datetime. import timedelta, datetime
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'hw_2_s-afanasev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime(2022, 1, 1),
) as dag:

    def print_date(ds):
        print(ds)

    t1 = BashOperator(
        task_id='print dir',
        bash_command='pwd',
    )

    t2 = PythonOperator(
        task_id='print_date',
        python_callable=print_date,
    )

    t1 >> t2
