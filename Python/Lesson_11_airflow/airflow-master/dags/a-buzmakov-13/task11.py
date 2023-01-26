from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook

postgres = PostgresHook(postgres_conn_id="startml_feed")

def top_user_connect():
    with postgres.get_conn() as conn:   
        with conn.cursor() as cursor:
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
            return cursor.fetchall()

with DAG(
    'a-buzmakov-13_task_11',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description='a-buzmakov-13_DAG_task11',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 10, 20),
    catchup=False,
    tags=['task_11'],
) as dag:               
    a = PythonOperator(
        task_id="top_user_by_con",
        python_callable=top_user_connect
    )

