from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from airflow.models import Variable

with DAG(
    'm-zaminev_task_12',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='m-zaminev_task_12',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 11, 16),
    catchup=False,
    tags=['m-zaminev_task_12']

) as dag:

    def print_variable():
        res = Variable.get("is_startml")
        print(res)

    t1 = PythonOperator(
        task_id='print_variable',
        python_callable=print_variable,
    )