from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent
from airflow.providers.postgres.operators.postgres import PostgresHook

with DAG(
        'i-morkovkin_hw_12',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='Exercise No.12',
        start_date=datetime(2022, 6, 14)
) as dag:

    def get_variable():
        from airflow.models import Variable

        is_start_ml = Variable.get("is_startml")
        print(is_start_ml)

    t = PythonOperator(
        task_id="print_variable",
        python_callable=get_variable
    )

    t
