from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook
from datetime import timedelta, datetime

with DAG(
    '11_10_mishin',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='hw 10',
    schedule_interval=timedelta(days=7),
    start_date=datetime(2022, 9, 21),
    catchup=False,
    tags=['m-mishin']
) as dag:

        def connect():
                postgres = PostgresHook(postgres_conn_id="startml_feed")
                with postgres.get_conn() as conn:  # вернет тот же connection, что вернул бы psycopg2.connect(...)
                        with conn.cursor() as cursor:
                                cursor.execute(f"""
                                SELECT DISTINCT user_id, COUNT(action)
                                FROM feed_action f
                                JOIN "user" u on f.user_id = u.id
                                WHERE action = 'like'
                                GROUP BY user_id
                                ORDER BY count DESC
                                """)
                                result = cursor.fetchone()
                return result

        t1 = PythonOperator(
                task_id='connect',
                python_callable=connect
        )