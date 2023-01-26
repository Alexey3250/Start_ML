from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'task_2_aktanova_b',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 31),
    catchup=False,
    tags=['example'],
) as dag:
    t1 = BashOperator(task_id='pwd', bash_command='pwd')

    def print_ds(ds):
        print(ds)

    t2 = PythonOperator(task_id='print_ds', python_callable=print_ds)

    t1 >> t2
