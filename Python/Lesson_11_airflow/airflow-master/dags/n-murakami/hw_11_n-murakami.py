from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def get_connection():
  from airflow.providers.postgres.operators.postgres import PostgresHook
  postgres = PostgresHook(postgres_conn_id="startml_feed")
  
  query = """
    SELECT
    user_id,
    COUNT(*) as count
    FROM feed_action 
    WHERE action='like'
    GROUP BY user_id
    ORDER BY COUNT(*) DESC
    LIMIT 1
    """
  with postgres.get_conn() as conn:
    with conn.cursor() as cursor:
      cursor.execute(query)
      result = cursor.fetchone()
  return result

with DAG(
        'hw_11_n-murakami',
        default_args={
          'depends_on_past': False,
          'retries': 3,
          'retry_delay': timedelta(minutes=5),
        },
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['hw_11_n-murakami']
) as dag:
  t= PythonOperator(
    task_id="get_user",
    python_callable=get_connection
  )
  


