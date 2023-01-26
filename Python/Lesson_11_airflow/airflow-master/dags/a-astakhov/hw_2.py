from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


def print_context(ds=None, **kwargs):
    print(ds)


with DAG(
    'Task_2',
    # Параметры по умолчанию
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime.now(),
    tags=['Task_2'],
) as dag:

    t1 = BashOperator(
        task_id='print_cur_dir',
        bash_command='pwd',
    )

    t2 = PythonOperator(
        task_id='print_the_context',
        python_callable=print_context,
    )

    t1 >> t2
