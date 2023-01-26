from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'a-grohovskaja-9_hw_2',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
    description='First DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['a-grohovskaja-9_hw_2'],
) as dag:

    t1 = BashOperator(
        task_id='print_directory',
        bash_command='pwd',
    )

    def print_arg(ds):
        print(ds)

    t2 = PythonOperator(
        task_id='print_smthn',
        python_callable=print_arg
    )

    t1 >> t2
