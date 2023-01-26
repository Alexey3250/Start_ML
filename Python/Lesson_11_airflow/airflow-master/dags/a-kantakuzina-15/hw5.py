"""\
# DAG documentation    
You can document your dag using markdown:    
`code`        
_курсив_    
__bold__    
"""
from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'kant_hw3',
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
    description='second DAG',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG. Каждый DAG "видит" свою "дату запуска" - это когда он предположительно должен был запуститься.
    # Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 12, 24),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['kant'],
) as dag:
 
    templated_command = dedent(
    """
    {% for i in range(5) %}
    echo "{{ ts }}"
    echo "{{ run_id }}"
    {% endfor %}
    """
    )

    t1 = BashOperator(
    task_id='templated',
    depends_on_past=False,
    bash_command=templated_command,
    )

    t1.doc_md = dedent(
    """\
    # DAG documentation    
    You can document your task using markdown:    
    `code`        
    _курсив_    
    __bold__    
    """
    ) 

    dag.doc_md = dedent(__doc__)  # Можно забрать докстрингу из начала файла вот так
    # чтобы можно было использовать разметку языка Markdown используем функцию dedent


    t1

