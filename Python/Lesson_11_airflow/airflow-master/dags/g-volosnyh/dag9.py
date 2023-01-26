from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator

from airflow.providers.postgres.operators.postgres import PostgresHook


def my_req():
    postgres = PostgresHook(postgres_conn_id='startml_feed')
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT f.user_id, COUNT(f.user_id)
                FROM feed_action f
                WHERE f.action = 'like'
                GROUP BY f.user_id
                ORDER BY COUNT(f.user_id) DESC
                LIMIT 1
                """
            )
            result = cursor.fetchall()
            return result


with DAG(
    'dag9_g-volosnyh',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='dag9',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id=f'get_maxlike_user',
        python_callable=my_req,
    )
