from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook

from psycopg2.extras import RealDictCursor

with DAG(
    'hw_11_p-tarabanov',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    
    description='11 task',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 9, 11),
    catchup=False,
    tags=['startml_feed'],
) as dag:
    
    def query_user_max_like():
        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
              with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""                   
                                    select feed_action.user_id as user_id, count(feed_action.action) as count
                                    from feed_action
                                    where feed_action.action='like'
                                    group by feed_action.user_id
                                    order by count desc
                                    limit 1
                                   """)
                    results = cursor.fetchone()
        return results
    
    
    task1 = PythonOperator(
        task_id = 'task_query_user_max_like',
        python_callable = query_user_max_like
        
    )
    
    task1