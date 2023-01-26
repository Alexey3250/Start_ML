from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

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
        'HW_11_d-lobachev_Connections',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='DAG step 11 of HW Lesson 11',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 2, 10),
        catchup=False,
        tags=['lobachev'],
) as dag:
    t1 = PythonOperator(
        task_id='get_top_user',
        python_callable=get_connection,
    )

    t1

