from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook


with DAG(
    'hw_11_v-didovik',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='hw_11_v-didovik',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_11_v-didovik'],
) as dag:

    def get_user_put_max_likes():
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
        task_id='get_user_put_max_likes',
        python_callable=get_user_put_max_likes,
    )
