from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from psycopg2.extras import RealDictCursor

def user_id_max_like():
    from airflow.providers.postgres.operators.postgres import PostgresHook

    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT user_id, COUNT(action) AS count
                FROM "feed_action"
                WHERE action = 'like'
                GROUP BY user_id
                ORDER BY count DESC
                LIMIT 1
                """
            )
            return cursor.fetchone()

with DAG(
        'hw_11_d-gusev',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='DAG for hw_11',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 8, 13),
        catchup=False,
        tags=['hw_11']
) as dag:
    
    user_max_like = PythonOperator(
        task_id='user_id_max_like',
        python_callable=user_id_max_like,
    )

    user_max_like.doc_md = dedent(
        """
        ## Напишите DAG, состоящий из одного `PythonOperator`.
        Этот оператор должен, используя подключение с `conn_id="startml_feed"`,
        **найти пользователя**, который поставил больше всего лайков,
        и вернуть словарь `{'user_id': <идентификатор>, 'count': <количество лайков>}`.
        """
    )

    user_max_like