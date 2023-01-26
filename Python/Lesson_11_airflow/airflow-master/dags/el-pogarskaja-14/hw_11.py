from airflow import DAG
from textwrap import dedent
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook

#with PostgresHook sessions with proper parameters are created automatically
def request():
    #gets connection
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:
    #makes request   
        with conn.cursor() as cursor:
            cursor.execute(
                    """
                    SELECT f.user_id, COUNT(f.user_id)
                    FROM feed_action f
                    WHERE f.action = 'like'
                    GROUP BY f.user_id
                    ORDER BY COUNT(f.post_id) DESC
                    LIMIT 1
                    """)
            result = cursor.fetchall()
            return result
with DAG(
        'el-pogarskaja-14_11',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
            },
        description='A simple tutorial DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2021, 1, 1),
        catchup=False,
        ) as dag:
    t1 = PythonOperator(
            task_id = 'top_likes_request',
            python_callable = request,
            )



