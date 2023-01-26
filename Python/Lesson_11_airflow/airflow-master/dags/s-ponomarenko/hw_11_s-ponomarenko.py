# s-ponomarenko мой логин
import time
import requests
import json

from datetime import datetime, timedelta
from textwrap import dedent

#Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

#Операторы - это кирпичики DAG, оня являются звеньями в графе.
#Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def get_connection():
    from airflow.hooks.base import BaseHook
    import psycopg2
    from psycopg2.extras import RealDictCursor

    creds = BaseHook.get_connection("startml_feed")
    with psycopg2.connect(
            f"postgresql://{creds.login}:{creds.password}"
            f"@{creds.host}:{creds.port}/{creds.schema}", cursor_factory=RealDictCursor
    ) as conn:
        with conn.cursor() as cursor:

            # Делаем запрос
            cursor.execute("""                   
                SELECT user_id, count(*)
                FROM feed_action
                WHERE action = 'like'
                GROUP BY user_id
                ORDER BY count(*) DESC
                LIMIT 1
            """)
            results = cursor.fetchone()  # Получаем результаты (fetchall() - "получить всё")
            results  # Это будет стандартный Python-объект. Не очень удобно, но работает
            return results


with DAG(
    'connection_dag_test',
    #Параметры по умолчанию для тасков
    default_args={
        # если прошлые запуски упали, надо ли ждать их успеха
        'depends_on_past': False,
        # Кому писать при провале
        'email': ['airflow@example.com'],
        # А писать ли вообще при провале
        'email_on_failure': False,
        # писать ли при автоматическом перезапуске при провале
        'email_on_retry': False,
        # Сколько раз пытаться запустить, далее помечать как failed
        'retries': 1,
        # Сколько ждать между перезапусками
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    # Описание DAG (не тасков, а самого DAG)
    description = 'DAG for home work - step 11',
    # Как часто запускать DAG (days=1 - каждый день)
    schedule_interval = timedelta(days=1),
    # С какой даты начать запускать DAG.
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно был запуститься.
    # Не всегда совпадает с датой на вашем компьютере.
    start_date = datetime(2022,11,10),
    # Запустить за старые даты относительно сегодня
    catchup=False,
    # Теги, способы помечать DAG
    tags = ['xcom'],
) as dag:

    # пишем таски
    return_results_sql = PythonOperator(
        task_id = 'connection_test',
        python_callable = get_connection
    )

    # Указываем для DAG последовательность задач

    return_results_sql