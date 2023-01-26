from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


with DAG(
    'j-jancharskaja_2',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description = 'My first DAG',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 11, 11),
    catchup = False,
    tags = ['first']
) as dag:

    t1 = BashOperator(
        task_id = 'pwd',
        bash_command = 'pwd'
    )

    def print_context(ds, **kwargs):
        print(kwargs)
        print(ds)
        return 'Whatever you return gets printed in the logs'

    t2 = PythonOperator(
        task_id = 'print_ds',
        python_callable = print_context
    )

    t1 >> t2
