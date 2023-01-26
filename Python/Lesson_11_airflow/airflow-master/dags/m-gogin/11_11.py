from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook
from datetime import timedelta, datetime


def sql_conn():
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                    select user_id,count(*) from feed_action
                    where action = 'like'
                    group by user_id
                    order by count(*) desc
                    limit 1
                """
            )
            return cursor.fetchall()


with DAG(
        'hw_11_m-gogin',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),

        },
        description='hw_11_m-gogin',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 7, 25),
        catchup=False,
        tags=['hw_11'],
) as dag:
    t1 = PythonOperator(
        task_id='11_11',
        python_callable=sql_conn,
    )

    t1
