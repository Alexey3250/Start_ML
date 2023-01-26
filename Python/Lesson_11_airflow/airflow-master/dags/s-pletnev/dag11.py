from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from psycopg2.extras import RealDictCursor
import psycopg2


def get_user():
    creds = BaseHook.get_connection("startml_feed")
    with psycopg2.connect(
            f"postgresql://{creds.login}:{creds.password}"
            f"@{creds.host}:{creds.port}/{creds.schema}",
            cursor_factory=RealDictCursor
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT user_id, COUNT(*)
                FROM "feed_action" 
                WHERE action = 'like'
                GROUP BY user_id
                ORDER BY COUNT(*) DESC
                LIMIT 1
            """)

            return cursor.fetchone()


with DAG(
        's_pletnev_task_11_1',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='task_11_dag',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 10, 21),
        catchup=False,
        tags=['task_11'],
) as dag:
    task = PythonOperator(
        task_id="get_user_likes",
        python_callable=get_user
    )
