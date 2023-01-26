'''
Напишите DAG, состоящий из одного PythonOperator. Этот оператор должен, используя подключение с conn_id="startml_feed",
найти пользователя, который поставил больше всего лайков,
и вернуть словарь {'user_id': <идентификатор>, 'count': <количество лайков>}.
'''

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from airflow.providers.postgres.operators.postgres import PostgresHook



def get_sql_query():
  postgres = PostgresHook(postgres_conn_id='startml_feed')
  with postgres.get_conn() as conn:
    with conn.cursor() as cursor:
      cursor.execute(
        '''
        SELECT user_id, COUNT(action)
        FROM feed_action
        WHERE action = 'like'
        GROUP BY user_id
        ORDER BY COUNT(action) DESC
        LIMIT 1      
        '''
      )
      result = cursor.fetchone()
      return result


default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'DAG_HW_11_ponomareva',
    default_args=default_args,
    description='DAG for HW_11',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 1),
    catchup=False,
    tags=['ponomareva'],
) as dag:

  task_sql = PythonOperator(
    task_id='sql_query',
    python_callable=get_sql_query,
  )

  task_sql


