from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'step_2_mishura',

        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },

        description='step_2',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 7, 20),
        catchup=False,
        tags=['step_2'],
) as dag:

    def print_ds(ds):
        print(ds)


    t1 = BashOperator(
        task_id='print_dir',
        bash_command='pwd'
    )

    t2 = PythonOperator(
        task_id='print_date',
        python_callable=print_ds
    )

    t1 >> t2

