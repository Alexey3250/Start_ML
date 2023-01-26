from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresHook
from airflow.operators.python import PythonOperator


with DAG(
    'hw_9',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='exercise 11',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 18),
    catchup=False,
    tags=['postgres'],
) as dag:

    def get_active_user():
        from psycopg2.extras import RealDictCursor
        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:
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
        task_id="postgres_query",
        python_callable=get_active_user
    )
