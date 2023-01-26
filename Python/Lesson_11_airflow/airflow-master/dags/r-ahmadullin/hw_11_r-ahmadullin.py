from datetime import datetime, timedelta  # 10
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook
from psycopg2.extras import RealDictCursor


# cursor_factory=RealDictCursor

def postgres_connect():
    postgres = PostgresHook(postgres_conn_id='startml_feed')
    with postgres.get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                "select user_id, count(post_id) from feed_action where action = 'like' group by user_id order by count(post_id) desc limit 2")
            return cursor.fetchone()


with DAG(
        'hw11',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='A simple tutorial DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 13),
        catchup=False,
        tags=['no_tags'],
) as dag:
    t1 = PythonOperator(task_id='PythonOperator', python_callable=postgres_connect)

    t1