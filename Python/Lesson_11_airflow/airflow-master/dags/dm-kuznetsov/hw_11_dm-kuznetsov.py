from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.python import PythonOperator

def get_connection():
    from airflow.providers.postgres.operators.postgres import PostgresHook

    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                    select user_id,count(*) from feed_action
                    where action = 'like'
                    group by user_id
                    order by count(*) desc
                    limit 1                    
                """
            )
            return cursor.fetchall()

with DAG(
        'dm-kuznetsov_hw_11',
        # Параметры по умолчанию для тасок
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),

        },
        description='dm-kuznetsov_hw_11',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 3),
        catchup=False,
        tags=['task_1'],
) as dag:

    t1 = PythonOperator(
        task_id='get_top_user',
        python_callable=get_connection,
    )

    t1