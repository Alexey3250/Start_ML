from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook
import psycopg2.extras


with DAG (
    'task_11_e-grants',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
},
    description='DAG for task_11',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 10, 22),
    catchup=False,
    tags=['task_11'],
) as dag:
    
    def get_connection():

        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:  
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(
            """
            SELECT f.post_id, COUNT(f.post_id)
            FROM feed_action AS f
            WHERE f.action = 'like'
            GROUP BY f.post_id
            ORDER BY COUNT(f.post_id) DESC
            LIMIT 1
            """
            )
                result = cursor.fetchone()
                return result

    
    def get_xcom_pull(ti):
        
        ti.xcom_pull(key="return_value", task_ids="display_task_11")
        
    task = PythonOperator(task_id = "display_task_11", python_callable=get_xcom_pull)
    
    task