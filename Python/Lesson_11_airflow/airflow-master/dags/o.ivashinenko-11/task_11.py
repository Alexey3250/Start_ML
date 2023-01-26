"""
Test documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG
#from airflow.models import Variable

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


task_number = 11
state = 'wa'

with DAG(
    f'logofios_task_{task_number}',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,  # Если прошлые запуски упали, надо ли ждать их успеха
        'email': ['logofios@gmail.com'],  # Кому писать при провале
        'email_on_failure': False,  # А писать ли вообще при провале?
        'email_on_retry': False,  # Писать ли при автоматическом перезапуске по провалу
        'retries': 1,  # Сколько раз пытаться запустить, далее помечать как failed
        # Сколько ждать между перезапусками
        'retry_delay': timedelta(minutes=5),
    },
    description='Task 5',  # Описание DAG (не тасок, а самого DAG)
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 20),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=[f'logofios_task_{task_number}'],
) as dag:
    
    
    def get_user():
        from airflow.providers.postgres.operators.postgres import PostgresHook

        postgres = PostgresHook(postgres_conn_id='startml_feed')
        with postgres.get_conn() as conn: 
            with conn.cursor() as cursor:
                cursor.execute(f"""                   
                SELECT user_id, COUNT(action) AS count FROM "feed_action" WHERE action='like' GROUP BY user_id ORDER by 2 DESC LIMIT 1
                """)
                results = cursor.fetchone()
                
        return results
                

    task_conn = PythonOperator(
        task_id='task_python_set',
        python_callable=get_user
    )

    task_conn
