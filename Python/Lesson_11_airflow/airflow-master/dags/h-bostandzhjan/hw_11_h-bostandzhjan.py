from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from airflow.hooks.base_hook import BaseHook
from psycopg2.extras import RealDictCursor

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('h-bostandhzjan',
    default_args=default_args,
    description='A simple tutorial DAG_10',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['h-bostandzhjan']
         ) as dag:

    def like_user():
        import psycopg2
        creds = BaseHook.get_connection("startml_feed")
        with psycopg2.connect(
                f"postgresql://{creds.login}:{creds.password}"
                f"@{creds.host}:{creds.port}/{creds.schema}"
        ) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""SELECT user_id,          
                                COUNT(action) 
                                FROM "feed_action"          
                                WHERE feed_action.action = 'like'
                                GROUP BY user_id
                                ORDER BY COUNT(action) DESC
                                LIMIT 1
                                """)
                return cursor.fetchone()


    t1 = PythonOperator(
        task_id="t1_like_user",
        python_callable=like_user)

    t1

