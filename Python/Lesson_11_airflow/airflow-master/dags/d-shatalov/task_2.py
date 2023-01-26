from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'task_2_d-shatalov',
        default_args={
            'depands_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description="task_2",
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 9, 21),  # логическая дата
        catchup=False,
        tags=['d-shatalov']
) as dag:

    t1 = BashOperator(
        task_id='print_dir',
        bash_command='pwd')

    def print_contex(ds):
        print(ds)
        return "whatever"
    t2 = PythonOperator(
    task_id='print_date',
    python_callable=print_contex)

t1 >> t2