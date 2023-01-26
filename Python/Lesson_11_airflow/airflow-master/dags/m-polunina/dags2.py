from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG('polunina_2',
default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
    # Описание DAG (не тасок, а самого DAG)
    description='A unit 2',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 12, 20),
    catchup=False,
    tags=['A unit 2'],
) as dag:

    t1 = BashOperator(task_id = 'bash_2', bash_command = 'pwd')

    def print_ds(ds, **kwargs):
        print(kwargs)
        print(ds)
        return 'Whatever you return gets printed in the logs'

    t2 = PythonOperator(task_id = 'python_2', python_callable = print_ds)

    t1 >> t2
