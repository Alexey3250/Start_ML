from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from textwrap import dedent

# from airflow.hooks.base import BaseHook
# import psycopg2
#
# creds = BaseHook.get_connection(id соединения)
# with psycopg2.connect(
#         f"postgresql://{creds.login}:{creds.password}"
#         f"@{creds.host}:{creds.port}/{creds.schema}"
# ) as conn:
#     with conn.cursor(cursor_factory=RealDictCursor) as cursor:
#         ...
#         # your code

from airflow.providers.postgres.operators.postgres import PostgresHook

with DAG(
        'hw_10_DAG_nivanova',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='First task DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 6, 18),
        catchup=False,
        tags=['hw'],
) as dag:

    def get_active_user():
        from psycopg2.extras import RealDictCursor
        postgres = PostgresHook(postgres_conn_id="startml_feed")
        # .get_conn() работает схоже с psycopg2
        with postgres.get_conn() as conn:
            # как и в psycopg2, необходимо создавать курсор
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT f.user_id, COUNT(f.user_id)
                    FROM feed_action f
                    WHERE f.action = 'like'
                    GROUP BY f.user_id
                    ORDER BY COUNT(f.user_id) DESC
                    LIMIT 1
                    """
                )
                results = cursor.fetchone()
        return results

    t1 = PythonOperator(
        task_id='get_user_data',
        python_callable=get_active_user,
    )