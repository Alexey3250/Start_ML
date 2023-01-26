from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook

postgres = PostgresHook(postgres_conn_id="startml_feed")

def get_user():
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                '''
                SELECT user_id, COUNT(action) as count FROM 
                feed_action GROUP BY user_id, action
                HAVING action = 'like' ORDER BY count DESC
                LIMIT 1
                '''
            )
            res = cursor.fetchone()
            return res
with DAG(
	'hw_11',
	default_args={
		'depends_on_past': False,
		'email': ['airflow@example.com'],
		'email_on_failure': False,
		'email_on_retry': False,
		'retries': 1,
		'retry_delay': timedelta(minutes=5)
	},  
    start_date=datetime(2022, 12, 26)
) as dag:
	    t1 = PythonOperator(
		    task_id='get_user',
		    python_callable = get_user
	    )
