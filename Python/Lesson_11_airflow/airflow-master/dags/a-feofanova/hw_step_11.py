from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

def find_user_from_db():

    from airflow.hooks.base import BaseHook
    import psycopg2
    from psycopg2.extras import RealDictCursor

    connection = BaseHook.get_connection('startml_feed')
    with psycopg2.connect(
        f"postgresql://{connection.login}:{connection.password}"
        f"@{connection.host}:{connection.port}/{connection.schema}",
        cursor_factory=RealDictCursor
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                SELECT user_id, COUNT(action)
                FROM feed_action
                WHERE action = 'like'
                GROUP BY(user_id)
                ORDER BY (COUNT(action)) DESC
                LIMIT 1
                """)
                result = cursor.fetchone()
                return result

with DAG(
    'a-feofanova_lesson_11_hw_step_11',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
    description = 'DAG_to_find_user',
    schedule_interval = timedelta(days = 1),
    start_date = datetime(2022, 12, 14),
    catchup = False,
    tags = ['DAG with python operator'],
) as dag:

    task = PythonOperator(
        task_id = 'find_user',
        python_callable = find_user_from_db,
    )
