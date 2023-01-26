from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook

with DAG(
        'm-zaminev_task_11',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='m-zaminev_task_11',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 11, 16),
        catchup=False,
        tags=['m-zaminev_task_11'],
) as dag:

    def max_likes():
        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute('''
                    SELECT user_id, COUNT(user_id)
                    FROM feed_action
                    WHERE action = 'like'
                    GROUP BY user_id
                    ORDER BY COUNT(user_id) DESC
                    LIMIT 1 
                    '''
                )
                result = cursor.fetchone()
                return result

    t1 = PythonOperator(
        task_id='max_likes',
        python_callable=max_likes,
    )

    t1