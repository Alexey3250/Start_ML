from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook

with DAG(
        'connect_dag_pobol',
        default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
    start_date = datetime(2022,12,24),
    schedule_interval = timedelta(days=1)
    ) as dag:

    def get_user():
        postgres = PostgresHook(postgres_conn_id="startml_feed")
        
        with postgres.get_conn() as conn:
            with conn.cursor() as cursor:
            
                cursor.execute("""
                        SELECT user_id, COUNT(action) as count
                        FROM feed_action
                        WHERE action = 'like'
                        GROUP BY user_id
                        ORDER BY COUNT(action) desc
                        LIMIT 3
                           """)
                return cursor.fetchone()


    python_task = PythonOperator(
            task_id = 'get_user',
            python_callable = get_user
            )


