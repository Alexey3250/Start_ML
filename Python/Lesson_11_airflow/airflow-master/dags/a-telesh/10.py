from airflow import DAG
from airflow.operators.python import PythonOperator
from psycopg2.extras import RealDictCursor
from airflow.providers.postgres.operators.postgres import PostgresHook
from datetime import datetime, timedelta


def most_likes_user():
    postgres = PostgresHook(postgres_conn_id='startml_feed')
    with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                with t as (select user_id, COUNT(*) as c
                from "feed_action"
                where action='like'
                group by user_id
                )
                SELECT user_id, c as count
                from t
                where c = (SELECT MAX(c) from t)
                """)
            return cursor.fetchone()


with DAG(
    'hw_10_a-telesh',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='HW 10',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 22),
    catchup=False,
    tags=['At']
) as dag:
    t1 = PythonOperator(
        task_id='t_return',
        python_callable=most_likes_user
    )

    t1
