from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from psycopg2.extras import RealDictCursor


def get_top_user_by_likes():
    from airflow.providers.postgres.operators.postgres import PostgresHook

    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT user_id, COUNT(user_id) FROM "feed_action"
                WHERE action = 'like' GROUP BY user_id
                ORDER BY COUNT(user_id) DESC
                LIMIT 1
                """
            )
            return cursor.fetchone()


with DAG(
        'hw_11_j-filatov-13',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='hw_11_exercise_11',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 10, 22),
        catchup=False,
        tags=['hw_11']
) as dag:

    t1 = PythonOperator(
        task_id='get_top_user_by_likes',
        python_callable=get_top_user_by_likes,
    )

    t1
