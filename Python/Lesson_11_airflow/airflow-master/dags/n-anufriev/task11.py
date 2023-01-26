from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

with DAG(
        'hw_11_n-anufriev',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)},
        description='anufriev_lesson11',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 11, 1),
        catchup=False,
        tags=['hw_11_n-anufriev']
) as dag:

    def get_user():
        postgres = PostgresHook(postgres_conn_id='startml_feed')
        with postgres.get_conn(cursor_factory=RealDictCursor) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""                   
                    with tmp as (
                        SELECT user_id, count(action) as count
                        FROM feed_action
                        WHERE action='like'
                        GROUP BY user_id
                        ) 
                        select user_id, count
                    from tmp
                        WHERE count = (select max(count) from tmp)
                            """)
                results = cursor.fetchall()
                return results

    get_user_ = PythonOperator(
        task_id='search_for_user_id',
        python_callable=get_user
    )
