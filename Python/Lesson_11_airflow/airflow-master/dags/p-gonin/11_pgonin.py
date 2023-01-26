from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'hw11_pgonin',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    }
    ,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['pgonin']
) as dag:


    def get_result():
        from airflow.providers.postgres.operators.postgres import PostgresHook

        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
            with conn.cursor() as cursor:
                cursor.execute(
                f"""
                SELECT 
                    user_id,
                    count(*) as count
                FROM feed_action
                WHERE action = 'like'
                GROUP BY user_id 
                ORDER BY count(*) DESC
                LIMIT 1
                """
                )
                return cursor.fetchone()

    t1 = PythonOperator(
        task_id='get_res',  
        python_callable=get_result
    )
    
    t1
