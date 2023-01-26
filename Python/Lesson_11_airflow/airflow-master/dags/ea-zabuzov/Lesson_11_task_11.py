from airflow.providers.postgres.operators.postgres import PostgresHook
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

postgres = PostgresHook(postgres_conn_id="startml_feed")


def query_from_postgres():
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute('''
            SELECT user_id,
                count(action) AS count
            FROM feed_action
            WHERE action = 'like'
            GROUP BY user_id
            ORDER BY count DESC
            LIMIT 1
            ''')
            return cursor.fetchall()

with DAG(
        'Lesson_11_step_11',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='Connection to Postgre base',
        start_date=datetime(2022, 6, 15),
        schedule_interval=timedelta(days=1),
        catchup=False,
        tags=['e.zabuzov', 'step_11']
) as dag:
    t1 = PythonOperator(
        task_id='Hook_from_postgres',
        python_callable=query_from_postgres
    )
