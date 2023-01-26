from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}


def extract_user_postgres():
    from airflow.hooks.base import BaseHook
    import psycopg2

    creds = BaseHook.get_connection(conn_id="startml_feed")
    with psycopg2.connect(
            f"postgresql://{creds.login}:{creds.password}"
            f"@{creds.host}:{creds.port}/{creds.schema}"
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
            SELECT user_id, count(action) as count
            FROM feed_action
            where action = 'like'
            group by user_id
            order by count(action) desc
            LIMIT 1
            """)
        results = cursor.fetchone()
        needed_values = {"user_id": results[0], "count": results[1]}
        cursor.close()
    conn.close()
    return needed_values


with DAG(
        dag_id="task11_v-saharov-20",
        default_args=default_args,
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False
) as dag:
    extract_user = PythonOperator(
        task_id="extract_user",
        python_callable=extract_user_postgres
    )
