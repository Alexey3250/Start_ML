from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook

postgres = PostgresHook(postgres_conn_id="startml_feed")

def get_user_make_max_likes():
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT fa.user_id, COUNT(fa.user_id)
                FROM feed_action fa
                WHERE fa.action = 'like'
                GROUP BY fa.user_id
                ORDER BY COUNT(fa.user_id) DESC
                LIMIT 1       
                """
                )
            return cursor.fetchall()

with DAG(
    'hw_11_r-kulaev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='hw_11_r-kulaev',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 10, 29),
    catchup=False,
    tags=['hw_11_r-kulaev'],
) as dag:
    a = PythonOperator(
        task_id="user_make_max_likes",
        python_callable=get_user_make_max_likes
    )
