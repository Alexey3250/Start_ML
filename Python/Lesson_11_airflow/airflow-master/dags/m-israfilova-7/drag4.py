from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook

with DAG(
    'sqldag',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    }, 
    description='Just for practice',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=[ 'masha' ],
) as dag:

    def getdata():
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            curs.execute("SELECT f.user_id, count(f.user_id) from 'feed_action' f WHERE f.action = 'like' GROUP BY f.user_id ORDER BY count(f.user_id) DESC LIMIT 1" )
            answer = curs.fetchone()
    return answer 

    task = PythonOperator(
            task_id='task',  
            python_callable=getdata,
        )