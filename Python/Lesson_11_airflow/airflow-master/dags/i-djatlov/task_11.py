from textwrap import dedent
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.hooks.base import BaseHook
import psycopg2


with DAG(
    'hw_11_i-djatlov',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description='task_11',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 11, 1),
    catchup=False,
    tags=['i-djatlov']
) as dag:
    
    def find_user():
        creds = BaseHook.get_connection('startml_feed')
        with psycopg2.connect(
            f"postgresql://{creds.login}:{creds.password}"
            f"@{creds.host}:{creds.port}/{creds.schema}"
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                SELECT user_id, COUNT(user_id) as count
                FROM feed_action
                WHERE action = 'like'
                GROUP BY user_id
                ORDER BY count DESC
                LIMIT 1;
                """)
                result = cursor.fetchone()
                return result
    def return_user(ti):
        pull_func = ti.xcom_pull(
            key='return_value',
            task_ids='user_like'
        )
        print(pull_func)
    
    
    t1 = PythonOperator(
        task_id='user_like',
        python_callable=return_user,
    )