
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator



with DAG(
    'my_dag_task_3',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 31),
    catchup=False
) as dag:

    def connection():
        from airflow.providers.postgres.operators.postgres import PostgresHook
        postgres = PostgresHook(postgres_conn_id="startml_feed")
    
        with postgres.get_conn() as conn:  

            with conn.cursor() as cursor:
            
                cursor.execute("""                   
                    select user_id, count(action)
                    from feed_action
                    where action = 'like'
                    group by user_id
                    order by count(action) desc
                    limit 1
                    """)
                
                return cursor.fetchall()

    pg_query = PythonOperator(
        task_id = 'conn_to_pg',
        python_callable=connection
    )
    pg_query


