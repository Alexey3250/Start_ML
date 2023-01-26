from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.hooks.base import BaseHook
import psycopg2

with DAG(
    'Connection',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    start_date=datetime(2023, 1, 21),
    tags=['a-rybakovskaya'],

) as dag:
    def get_id():
        conn_id = "startml_feed"
        creds = BaseHook.get_connection(conn_id)
        with psycopg2.connect(
                f"postgresql://{creds.login}:{creds.password}"
                f"@{creds.host}:{creds.port}/{creds.schema}"
        ) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    cursor.execute(
                        """
                        SELECT user_id, COUNT(user_id)
                        FROM feed_action 
                        WHERE action = 'like'
                        GROUP BY user_id
                        ORDER BY COUNT(user_id) DESC
                        LIMIT 100;
                        """
                    )
                    result = cursor.fetchone()
        return result


    t1 = PythonOperator(
        task_id='get_id',
        python_callable=get_id)

    t1
