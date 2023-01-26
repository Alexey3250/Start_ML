#hello
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'task_2_breus',

default_args={

    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5), 
},

    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 31),
    catchup=False
) as dag:

    t1 = BashOperator(task_id='pwd', bash_command='pwd')

    def print_ds(ds):
        print("it's logic date")
        print(ds)

    t2 = PythonOperator(task_id='print_ds', python_callable=print_ds)

    t1 >> t2
