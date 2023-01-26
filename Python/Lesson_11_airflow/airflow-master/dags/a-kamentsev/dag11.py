from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def get_connection():
    from airflow.hooks.base import BaseHook
    import psycopg2
    from psycopg2.extras import RealDictCursor

    creds = BaseHook.get_connection('startml_feed')
    with psycopg2.connect(
            f"postgresql://{creds.login}:{creds.password}"
            f"@{creds.host}:{creds.port}/{creds.schema}",
            cursor_factory=RealDictCursor
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id, COUNT(action='like') AS count
                FROM "feed_action"
                WHERE action='like'
                GROUP BY user_id
                ORDER BY count DESC
                LIMIT 1
                """)
            return dict(cursor.fetchone())


with DAG(
        'a-kamentsev_dag11',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 11, 11),
        catchup=False,
        tags=['a-kamentsev_dag11']
) as dag:
    t1 = PythonOperator(
        task_id='liker',
        python_callable=get_connection)