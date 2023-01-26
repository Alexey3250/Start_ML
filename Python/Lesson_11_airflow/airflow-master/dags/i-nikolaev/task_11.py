from airflow.providers.postgres.operators.postgres import PostgresHook
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from psycopg2.extras import RealDictCursor
with DAG(
    # Название
    'i_Nikolaev_task_11',
    # Параметры по умолчанию для тасок
    default_args={
        # Если прошлые запуски упали, надо ли ждать их успеха
        'depends_on_past': False,
        # Кому писать при провале
        'email': ['airflow@example.com'],
        # А писать ли вообще при провале?
        'email_on_failure': False,
        # Писать ли при автоматическом перезапуске по провалу
        'email_on_retry': False,
        # Сколько раз пытаться запустить, далее помечать как failed
        'retries': 1,
        # Сколько ждать между перезапусками
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    # Описание DAG (не тасок, а самого DAG)
    description='A simple tutorial DAG',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 1, 1),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['example'],
) as dag:
          def get_data():
            postgres = PostgresHook(postgres_conn_id="startml_feed")
            with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
              with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""                   
                SELECT user_id, count(user_id) count
                FROM feed_action
                WHERE action = 'like'
                GROUP BY user_id
                ORDER BY count DESC
                LIMIT 1
                """)
                results = cursor.fetchone()
                return dict(results)
          task1 = PythonOperator(
            task_id='The_first_task',  # в id можно делать все, что разрешают строки в python
            python_callable=get_data,
          )