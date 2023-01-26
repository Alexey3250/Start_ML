from datetime import timedelta, datetime
from airflow import DAG

from airflow.hooks.base import BaseHook
import psycopg2
from airflow.operators.python_operator import PythonOperator
from psycopg2.extras import RealDictCursor

def get_connection():
    creds = BaseHook.get_connection(conn_id="startml_feed")
    with psycopg2.connect(
            f"postgresql://{creds.login}:{creds.password}"
            f"@{creds.host}:{creds.port}/{creds.schema}", cursor_factory=RealDictCursor
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT user_id, COUNT(action='like')
                FROM feed_action
                WHERE action = 'like'
                GROUP BY user_id
                ORDER BY COUNT(action='like') DESC
                """)
            result = cursor.fetchone()
            return result


with DAG(
    '11_11_rbkvts',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
    description='hw_11',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 7, 22),
    catchup=False,
    tags=['11_2'],
) as dag:

    task = PythonOperator(
        task_id="conn_execute",
        python_callable=get_connection,
    )
