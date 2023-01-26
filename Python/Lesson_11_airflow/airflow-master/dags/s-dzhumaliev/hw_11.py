from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from psycopg2.extras import RealDictCursor

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook

def check_connection():
    postgres = PostgresHook(postgres_conn_id='startml_feed')
    with postgres.get_conn(cursor_factory=RealDictCursor) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT user_id, COUNT(*)
                FROM feed_action
                WHERE action = 'like'
                GROUP BY user_id
                ORDER BY COUNT(*) DESC
                LIMIT 1
            """)

            return cursor.fetchone()

with DAG(
    'HW_11_s-dzhumaliev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime(2022, 1, 1),
) as dag:
    PythonOperator(
        task_id='check_connection',
        python_callable=check_connection
    )