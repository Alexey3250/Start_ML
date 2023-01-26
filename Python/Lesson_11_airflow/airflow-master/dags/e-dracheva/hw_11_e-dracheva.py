from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

def postgr():
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
      with conn.cursor(cursor_factory=RealDictCursor) as cursor:
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
          data = cursor.fetchone()
    return data      

with DAG(

    'HW_11_e-dracheva',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
        },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['HW_11_e-dracheva'],
    ) as dag:
    
    t1 = PythonOperator(
        task_id="conn",
        python_callable=postgr
        )
    t1