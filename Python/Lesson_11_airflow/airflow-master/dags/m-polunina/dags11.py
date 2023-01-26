from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG('polunina_11',
default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},

    description='A unit 11',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 12, 20),
    catchup=False,
    tags=['A unit 11'],
) as dag:

    def feed_like():
        from airflow.providers.postgres.operators.postgres import PostgresHook
        postgres = PostgresHook(postgres_conn_id="startml_feed", cursor='dictcursor')
        with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
            with conn.cursor() as cursor:
                cursor.execute("""
            SELECT user_id, COUNT(user_id)
            FROM feed_action
            WHERE action = 'like'
            GROUP BY user_id
            ORDER BY count DESC
            LIMIT 1
            """)
                results = cursor.fetchone()
                return results

    t1 = PythonOperator(task_id = 'python_11', python_callable = feed_like)



