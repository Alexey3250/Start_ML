from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent
from airflow.providers.postgres.operators.postgres import PostgresHook

with DAG(
        'i-morkovkin_hw_11',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='Exercise No.11',
        start_date=datetime(2022, 6, 14)
) as dag:
    def get_values_from_db():
        from airflow.hooks.base import BaseHook
        import psycopg2

        creds = BaseHook.get_connection("startml_feed")
        with psycopg2.connect(
                f"postgresql://{creds.login}:{creds.password}"
                f"@{creds.host}:{creds.port}/{creds.schema}"
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    select user_id, count(post_id) as count
                    from feed_action
                    where action = 'like'
                    group by user_id
                    order by count(post_id) desc
                """)
                res = cursor.fetchone()
                d = {"user_id": res[0], "count": res[1]}
                return d

    task = PythonOperator(
        task_id="get_smth_from_db",
        python_callable=get_values_from_db
    )

    task
