from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook

with DAG(
        "hw_11_p-terentev-14",
        default_args={
            "depends_on_past": False,
            "email": ["airflow@example.com"],
            "email_on_failure": False,
            "email_on_retry": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5)
        },
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 11, 11),
        catchup=False,
        tags=['DAG_11']
) as dag:
    def get_user():
        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                            SELECT DISTINCT user_id, COUNT(action)
                            FROM feed_action f
                            JOIN "user" u on f.user_id = u.id
                            WHERE action = 'like'
                            GROUP BY user_id
                            ORDER BY count(action) DESC
                            LIMIT 1
                            """)
                result = cursor.fetchone()
        return result


    a1 = PythonOperator(
        task_id='get_user',
        python_callable=get_user
    )