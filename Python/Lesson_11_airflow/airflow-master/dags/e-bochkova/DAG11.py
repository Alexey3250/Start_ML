from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from psycopg2.extras import RealDictCursor


def get_data():
    from airflow.providers.postgres.operators.postgres import PostgresHook

    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect("startml_feed")
      with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(
            """
            SELECT f.user_id, COUNT(f.user_id)
            FROM feed_action f 
            WHERE f.action = 'like'
            GROUP BY f.user_id
            ORDER BY COUNT(f.user_id) DESC
            LIMIT 1
            """
        )
        data = cursor.fetchone()
    return data



with DAG(
    'hw_11_e-bochkova',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    # Описание DAG (не тасок, а самого DAG)
    description = 'A simple homework DAG',
    # Как часто запускать DAG
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 1, 1),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup = False,
    # теги, способ помечать даги
    tags = ['homework11'],
) as dag:

    t1 = PythonOperator(
        task_id = 'test_connection_db',
        python_callable = get_data
    )

