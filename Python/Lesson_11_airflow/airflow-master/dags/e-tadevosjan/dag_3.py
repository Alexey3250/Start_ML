from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'hw_3_e-tadevosjan',
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
    description='e-tadevosjan task 3',
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
    tags=['task_3'],
) as dag:
        def print_task(task_number):
                print(f'task number is: {task_number}')
        for i in range(10):
                bash_task = BashOperator(
                task_id='bash_task_'+str(i),
                bash_command=f'echo {i}')
                bash_task.doc_md = dedent("""
                **Bash task**
                ####Print `i` *cycle number*
                """)
        for i in range(20):
                python_task = PythonOperator(
                        task_id='python_task_'+str(i),
                        python_callable=print_task,
                        op_kwargs={'task_number':i})
                python_task.doc_md = dedent("""
                                **Python task**
                                ####Print `i` *cycle number*
                                """)
        bash_task>>python_task
