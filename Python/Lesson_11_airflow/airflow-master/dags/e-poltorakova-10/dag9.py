from airflow import DAG
from airflow.operators.python import PythonOperator
from textwrap import dedent
from datetime import datetime, timedelta

def max_like():
    from airflow.providers.postgres.operators.postgres import PostgresHook
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                    select user_id, count(*) from feed_action
                    where action = 'like'
                    group by user_id
                    order by 2 desc
                    limit 1                    
                """
                )
            return cursor.fetchone()

with DAG(
    'hm_11_e-poltorakova',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['e-poltorakova'],

) as dag:

    task = PythonOperator(
        task_id = 'max_like',
        python_callable=max_like,
    )

    task.doc_md = dedent(
        """
        Connects to the "startml" database using PostgresHook.
        At the output, it gives the user who made the most likes.
        """
        )

    task
