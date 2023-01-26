from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook

def find_data():
    import psycopg2
    from psycopg2.extras import RealDictCursor

    postgres = PostgresHook(postgres_conn_id='startml_feed')
    with postgres.get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("""
                SELECT user_id, COUNT(user_id)
                FROM feed_action
                WHERE action = 'like'
                GROUP BY user_id
                ORDER BY count(user_id) DESC
                LIMIT 1; 
            """
            )
            return cursor.fetchall()

# def get_data(ti):
#     print(
#         ti.xcom_pull(
#             key = 'return_value',
#             task_ids = 'store_xcom'
#         )
#     )

with DAG(
    'g-karateev_d10',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'A simple DAG for task 10',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 5, 5),
    catchup = False,
    tags = ['example']
) as dag:

    t1 = PythonOperator(
        task_id = 'store_xcom',
        python_callable = find_data,
    )
