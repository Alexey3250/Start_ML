from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook 
import psycopg2
from psycopg2.extras import RealDictCursor

def get_user_id():
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
            """
            SELECT user_id, 
                   COUNT(user_id) AS count
            FROM "feed_action"
            WHERE action = 'like'
            GROUP BY user_id
            ORDER BY count DESC
            LIMIT 1
            """
            )
            result = cursor.fetchone()
    return result

with DAG(
    'HW_11_a.platov',
    default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),         
        },
        description='Home Work N11 Connections',
        start_date=datetime(2022, 7, 24),
        catchup=False,
        tags=['a.platov'],
    ) as dag:
           
        t1 = PythonOperator(
            task_id='get_user_id',
            python_callable=get_user_id,
            )
        t1
