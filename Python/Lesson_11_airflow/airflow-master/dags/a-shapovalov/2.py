from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_2_a-shapovalov',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='Excercise 2 a-shapovalov',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_2_a-shapovalov']
) as dag:

    t1 = BashOperator(
        task_id='print_current_directory',
        bash_command='pwd')

    def print_context(ds, **kwargs):
        print(ds)

    t2 = PythonOperator(
        task_id='print_date',
        python_callable=print_context)

    t1 >> t2
