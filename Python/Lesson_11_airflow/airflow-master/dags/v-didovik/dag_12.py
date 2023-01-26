from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook
from airflow.models import Variable


with DAG(
    'hw_12_v-didovik',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='hw_12_v-didovik',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_12_v-didovik'],
) as dag:

    def get_var():
        is_startml = Variable.get("is_startml")
        print(is_startml)

    t1 = PythonOperator(
        task_id='get_var',
        python_callable=get_var,
    )
