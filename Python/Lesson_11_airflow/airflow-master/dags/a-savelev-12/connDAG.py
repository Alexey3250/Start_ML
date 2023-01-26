from datetime import datetime, timedelta
from textwrap import dedent
from urllib import request
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    }
def get_data():
    from airflow.providers.postgres.operators.postgres import PostgresHook
    postgres = PostgresHook(postgres_conn_id='startml_feed')
    with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
        with conn.cursor() as cursor:
            request = ( """select user_id, count(action)
                        from feed_action
                        where action = 'like'
                        group by user_id
                        order by count(action) desc
                        limit 1
                    """)
            cursor.execute(request)
            result = cursor.fetchone()  
            answer = {
            'user_id' : result[0],
            'count' : result[1]
            }
            return answer
with DAG(
    'hw_11_db_conn_a-savelev',
    default_args=default_args,
    start_date=datetime(2022, 9, 15),
    tags=['a-savelev-12'],
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:
    db_opr = PythonOperator(
        task_id='get_db_data',
        python_callable=get_data
    )
    db_opr