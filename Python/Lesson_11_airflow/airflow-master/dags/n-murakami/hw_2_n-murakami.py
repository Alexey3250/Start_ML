from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


def print_date(ds, **kwargs):
    print(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'

with DAG(
    'hw_2_n-murakami',
    default_args={
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_2_n-murakami'],
) as dag:

    t1 = BashOperator(
        task_id='show_directory',
        bash_command='pwd'
    )

    t2 = PythonOperator(
        task_id='show_ds',
        python_callable=print_date
    )

    t1 >> t2