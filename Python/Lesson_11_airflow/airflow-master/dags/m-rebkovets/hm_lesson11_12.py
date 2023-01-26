from datetime import timedelta, datetime
from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

with DAG(
    '11_12_rbkvts',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
    description='hw_12',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 7, 22),
    catchup=False,
    tags=['11_2'],
) as dag:

    def var():
        is_startml = Variable.get("is_startml")
        print(is_startml)

    task = PythonOperator(
        task_id='task_variable',
        python_callable=var
        )
