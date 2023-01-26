from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook

postgres = PostgresHook(postgres_conn_id="startml_feed")

def top_liker():
    with postgres.get_conn() as conn:
      with conn.cursor() as cursor:
          cursor.execute("""
                          SELECT f.user_id, COUNT(f.user_id)
                          FROM feed_action f
                          WHERE f.action = 'like'
                          GROUP BY f.user_id
                          ORDER BY COUNT(f.user_id) DESC
                          LIMIT 1            
                      """)
          return cursor.fetchall()

with DAG(
    'a-grohovskaja-9_hw_11',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
    description='First DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False
) as dag:
    t1 = PythonOperator(
        task_id='get_top_liker',
        python_callable=top_liker
    )

    t1
