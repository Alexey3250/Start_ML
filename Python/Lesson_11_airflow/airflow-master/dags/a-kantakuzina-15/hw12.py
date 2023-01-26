from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from airflow.providers.postgres.operators.postgres import PostgresHook
from psycopg2.extras import RealDictCursor


with DAG(
    'kant_hw12',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    },
    description='Exercise_12 DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 10),
    catchup=False,
    tags='kant_hw12',
) as dag:

    def get_variable():
        print(Variable.get('is_startml'))

    t1 = PythonOperator(
        task_id='get_variable',
        python_callable=get_variable,
        )

    t1