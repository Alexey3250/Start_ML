from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.hooks.base import BaseHook
import psycopg2


with DAG(
        's-duzhak-2-task_10',
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
        start_date=datetime(2022, 9, 11),
        catchup=False,
        tags=['example'],
) as dag:
    def get_best_user():
        creds = BaseHook.get_connection('startml_feed')
        with psycopg2.connect(
                f"postgresql://{creds.login}:{creds.password}"
                f"@{creds.host}:{creds.port}/{creds.schema}"
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT
                        user_id,
                        COUNT(*) as count
                    FROM feed_action
                    WHERE action = 'like'
                    group by user_id
                    ORDER BY COUNT(*) DESC
                    LIMIT 1
                """)
                res = cursor.fetchone()
                return {
                    'user_id': res[0],
                    'count': res[1]
                }

    t1 = PythonOperator(
        task_id='get_best_use',
        python_callable=get_best_user
    )




