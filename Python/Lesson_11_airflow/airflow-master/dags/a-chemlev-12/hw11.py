from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def get_top_user_by_likes():
    from airflow.providers.postgres.operators.postgres import PostgresHook

    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id, COUNT(action)
                FROM feed_action
                WHERE action = 'like'
                GROUP BY user_id
                ORDER BY COUNT(action) DESC
                LIMIT 1
                """
            )
            return cursor.fetchall()


with DAG(
        'chemelson_hw11',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='chemelson_hw11',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 9, 11),
        catchup=False,
        tags=['chemelson_hw11']
) as dag:
    t1 = PythonOperator(
        task_id="top_user_by_likes",
        python_callable=get_top_user_by_likes
    )
