from datetime import datetime, timedelta
from psycopg2.extras import RealDictCursor
import psycopg2

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook


default_args={
    'start_date': datetime(2022, 11, 18),
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

def get_user_with_most_likes():
    creds = BaseHook.get_connection("startml_feed")
    with psycopg2.connect(
    f"postgresql://{creds.login}:{creds.password}"
    f"@{creds.host}:{creds.port}/{creds.schema}",
    cursor_factory=RealDictCursor
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""SELECT user_id, COUNT(action) as count FROM "feed_action"
            WHERE "action"='like'
            GROUP BY user_id
            ORDER BY count DESC
            LIMIT 1
            """)
            result = cursor.fetchone()
            #result = dict(result)
            #print(result)
            return result

with DAG(
    "hw_11_v-gorohov_cool_dag",
    default_args=default_args,
    schedule_interval=None
) as dag:
    
    get_user = PythonOperator(
        task_id="push",
        python_callable=get_user_with_most_likes,
        dag=dag
    )