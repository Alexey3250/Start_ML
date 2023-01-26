from datetime import timedelta, datetime
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.providers.postgres.operators.postgres import PostgresHook
from psycopg2.extras import RealDictCursor

with DAG(
    'task11_d-gavlovskij',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='somedag',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 10, 31),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['gavlique']
) as dag:
    def get_top_liker():
        postgres = PostgresHook(postgres_conn_id='startml_feed')
        with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT user_id, count(1) as count
                    FROM feed_action
                    WHERE lower(action)='like'
                    GROUP BY user_id
                    ORDER BY count(1) DESC
                """)
                print(cursor.fetchone())

    t1 = PythonOperator(
        task_id='xcom_push',
        python_callable=get_top_liker,
        dag=dag
    )

    t1 
