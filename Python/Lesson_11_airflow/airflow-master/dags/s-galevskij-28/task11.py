from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.postgres.operators.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

postgres = PostgresHook(postgres_conn_id="startml_feed")
with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
  with conn.cursor(cursor_factory=RealDictCursor) as cursor:

      def base(cursor):
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
        dict = cursor.fetchone()
        return dict
      cursor.close()

      default_args = {
          'owner': 'airflow',
          'depends_on_past': False,
          'email_on_failure': False,
          'email_on_retry': False,
          'retries': 1,
          'retry_delay': timedelta(minutes=5)
      }

      with DAG(
              'task11_galevskii',
              start_date=datetime(2021, 1, 1),
              max_active_runs=2,
              schedule_interval=timedelta(minutes=30),
              default_args=default_args,
              catchup=False
      ) as dag:
          t1 = PythonOperator(
              task_id='most_likes',
              python_callable=base
          )
          t1