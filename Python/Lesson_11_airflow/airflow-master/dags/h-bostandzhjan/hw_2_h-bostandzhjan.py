from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('hw_1_h-bostandzjan',
         default_args=default_args,
         description='A simple tutorial DAG',
         schedule_interval=timedelta(days=1),
         start_date=datetime(2021, 1, 1),
         catchup=False,
         tags=['h-bostandzjan']
         ) as dag:
    t1 = BashOperator(
        task_id='pwd_command',
        bash_command='pwd')


    def print_ds(ds):
        print(ds)


    t2 = PythonOperator(
        task_id='python_t2',
        python_callable=print_ds
    )

    t1 >> t2