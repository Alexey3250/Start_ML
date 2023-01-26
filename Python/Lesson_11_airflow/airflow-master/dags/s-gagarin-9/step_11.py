from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def get_user_id():
    from airflow.providers.postgres.operators.postgres import PostgresHook

    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:  # вернет тот же connection, что вернул бы psycopg2.connect(...)
        with conn.cursor() as cursor:
            cursor.execute("""
            SELECT user_id, COUNT(action)
            FROM feed_action
            WHERE action = 'like'
            GROUP BY user_id
            ORDER BY COUNT(action) DESC
            LIMIT 1""")
            return cursor.fetchall()


with DAG(
    'step_11_gagarin',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='DAG_for_step_11',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 11, 12),
    catchup=False,
    tags=['step_11_gagarin']
) as dag:
    task = PythonOperator(
        task_id='get_info_user',
        python_callable=get_user_id

    )
