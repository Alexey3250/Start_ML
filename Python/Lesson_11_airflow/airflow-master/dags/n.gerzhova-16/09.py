from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook
import psycopg2
from psycopg2.extras import RealDictCursor


def get_liking_user():
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
              """ SELECT user_id, COUNT(action)
                  FROM feed_action
                  WHERE action = 'like'
                  GROUP BY user_id
                  ORDER BY COUNT(action) DESC
                  LIMIT 1""",
            )
            user = cursor.fetchone()
    return user

with DAG(
        'NG_nineth',
        default_args={
            'depends_on_past': False,
            'email': False,
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },

        description='DAG for Task 09',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 7, 23),
) as dag:    
    
    task = PythonOperator(
        task_id = 'get_the_user',
        python_callable=get_liking_user,
        )
