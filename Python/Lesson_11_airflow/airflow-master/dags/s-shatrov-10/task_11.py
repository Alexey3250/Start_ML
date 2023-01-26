from airflow import DAG
from datetime import timedelta
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook

def get_data():
    postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT
                         user_id
                        ,SUM(1) AS count
                    FROM
                        feed_action
                    WHERE 
                        action = 'like'
                    GROUP BY
                        user_id;
                """)
                return cursor.fetchall()

with DAG(
    'shatrov_task_11',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description="task_2",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 7, 27),
) as dag:

    t1 = PythonOperator(
        task_id="get_data",
        python_callable=get_data
    )
    
    t1