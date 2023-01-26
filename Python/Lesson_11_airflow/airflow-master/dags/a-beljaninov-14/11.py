from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from airflow.providers.postgres.operators.postgres import PostgresHook


def get_user_max_likes():
    postgres = PostgresHook(postgres_conn_id='startml_feed')
    with postgres.get_conn() as conn:  # вернет тот же connection, что вернул бы psycopg2.connect(...)
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


# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        'xcom_dag',
        start_date=datetime(2021, 1, 1),
        max_active_runs=2,
        schedule_interval=timedelta(minutes=30),
        default_args=default_args,
        catchup=False
) as dag:
    t1 = PythonOperator(
        task_id='get_user_max_likes',
        python_callable=get_user_max_likes,
    )
